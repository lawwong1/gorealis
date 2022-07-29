/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package realis

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/aurora-scheduler/gorealis/v2/gen-go/apache/aurora"
)

// Offers on [aurora-scheduler]/offers endpoint
type Offer struct {
	ID struct {
		Value string `json:"value"`
	} `json:"id"`
	FrameworkID struct {
		Value string `json:"value"`
	} `json:"framework_id"`
	AgentID struct {
		Value string `json:"value"`
	} `json:"agent_id"`
	Hostname string `json:"hostname"`
	URL      struct {
		Scheme  string `json:"scheme"`
		Address struct {
			Hostname string `json:"hostname"`
			IP       string `json:"ip"`
			Port     int    `json:"port"`
		} `json:"address"`
		Path  string        `json:"path"`
		Query []interface{} `json:"query"`
	} `json:"url"`
	Resources []struct {
		Name   string `json:"name"`
		Type   string `json:"type"`
		Ranges struct {
			Range []struct {
				Begin int `json:"begin"`
				End   int `json:"end"`
			} `json:"range"`
		} `json:"ranges,omitempty"`
		Role         string        `json:"role"`
		Reservations []interface{} `json:"reservations"`
		Scalar       struct {
			Value float64 `json:"value"`
		} `json:"scalar,omitempty"`
	} `json:"resources"`
	Attributes []struct {
		Name string `json:"name"`
		Type string `json:"type"`
		Text struct {
			Value string `json:"value"`
		} `json:"text"`
	} `json:"attributes"`
	ExecutorIds []struct {
		Value string `json:"value"`
	} `json:"executor_ids"`
}

// hosts on [aurora-scheduler]/maintenance endpoint
type MaintenanceList struct {
	Drained   []string            `json:"DRAINED"`
	Scheduled []string            `json:"SCHEDULED"`
	Draining  map[string][]string `json:"DRAINING"`
}

type OfferCount map[float64]int64
type OfferGroupReport map[string]OfferCount
type OfferReport map[string]OfferGroupReport

// MaintenanceHosts list all the hosts under maintenance
func (c *Client) MaintenanceHosts() ([]string, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: c.config.insecureSkipVerify},
	}

	request := &http.Client{Transport: tr}

	resp, err := request.Get(fmt.Sprintf("%s/maintenance", c.GetSchedulerURL()))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}

	var list MaintenanceList

	if err := json.Unmarshal(buf.Bytes(), &list); err != nil {
		return nil, err
	}

	hosts := append(list.Drained, list.Scheduled...)

	for drainingHost := range list.Draining {
		hosts = append(hosts, drainingHost)
	}

	return hosts, nil
}

// Offers pulls data from /offers endpoint
func (c *Client) Offers() ([]Offer, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: c.config.insecureSkipVerify},
	}

	request := &http.Client{Transport: tr}

	resp, err := request.Get(fmt.Sprintf("%s/offers", c.GetSchedulerURL()))
	if err != nil {
		return []Offer{}, err
	}
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}

	var offers []Offer

	if err := json.Unmarshal(buf.Bytes(), &offers); err != nil {
		return []Offer{}, err
	}

	return offers, nil
}

// AvailOfferReport returns a detailed summary of offers available for use.
// For example, 2 nodes offer 32 cpus and 10 nodes offer 1 cpus.
func (c *Client) AvailOfferReport() (OfferReport, error) {
	maintHosts, err := c.MaintenanceHosts()
	if err != nil {
		return nil, err
	}

	maintHostSet := map[string]bool{}
	for _, h := range maintHosts {
		maintHostSet[h] = true
	}

	// Get a list of offers
	offers, err := c.Offers()
	if err != nil {
		return nil, err
	}

	report := OfferReport{}

	for _, o := range offers {
		if maintHostSet[o.Hostname] {
			continue
		}

		group := "non-dedicated"
		for _, a := range o.Attributes {
			if a.Name == "dedicated" {
				group = a.Text.Value
				break
			}
		}

		if _, ok := report[group]; !ok {
			report[group] = map[string]OfferCount{}
		}

		for _, r := range o.Resources {

			if _, ok := report[group][r.Name]; !ok {
				report[group][r.Name] = OfferCount{}
			}

			val := 0.0
			switch r.Type {
			case "SCALAR":
				val = r.Scalar.Value
			case "RANGES":
				for _, pr := range r.Ranges.Range {
					val += float64(pr.End - pr.Begin + 1)
				}
			default:
				return nil, fmt.Errorf("%s is not supported", r.Type)
			}

			report[group][r.Name][val]++
		}
	}

	return report, nil
}

// FitTasks computes the number tasks can be fit in a list of offer
func (c *Client) FitTasks(taskConfig *aurora.TaskConfig, offers []Offer) (int64, error) {
	// count the number of tasks per limit contraint: limit.name -> limit.value -> count
	limitCounts := map[string]map[string]int64{}
	for _, c := range taskConfig.Constraints {
		if c.Constraint.Limit != nil {
			limitCounts[c.Name] = map[string]int64{}
		}
	}

	request := ResourcesToMap(taskConfig.Resources)

	// validate resource request
	if len(request) == 0 {
		return -1, fmt.Errorf("Resource request %v must not be empty", request)
	}

	isValid := false
	for _, resVal := range request {
		if resVal > 0 {
			isValid = true
			break
		}
	}

	if !isValid {
		return -1, fmt.Errorf("Resource request %v is not valid", request)
	}

	// pull the list of hosts under maintenance
	maintHosts, err := c.MaintenanceHosts()
	if err != nil {
		return -1, err
	}

	maintHostSet := map[string]bool{}
	for _, h := range maintHosts {
		maintHostSet[h] = true
	}

	numTasks := int64(0)

	for _, o := range offers {
		// skip the hosts under maintenance
		if maintHostSet[o.Hostname] {
			continue
		}

		numTasksPerOffer := int64(-1)

		for resName, resVal := range request {
			// skip as we can fit a infinite number of tasks with 0 demand.
			if resVal == 0 {
				continue
			}

			avail := 0.0
			for _, r := range o.Resources {
				if r.Name != resName {
					continue
				}

				switch r.Type {
				case "SCALAR":
					avail = r.Scalar.Value
				case "RANGES":
					for _, pr := range r.Ranges.Range {
						avail += float64(pr.End - pr.Begin + 1)
					}
				default:
					return -1, fmt.Errorf("%s is not supported", r.Type)
				}
			}

			numTasksPerResource := int64(avail / resVal)

			if numTasksPerResource < numTasksPerOffer || numTasksPerOffer < 0 {
				numTasksPerOffer = numTasksPerResource
			}
		}

		numTasks += fitConstraints(taskConfig, &o, limitCounts, numTasksPerOffer)
	}

	return numTasks, nil
}

func fitConstraints(taskConfig *aurora.TaskConfig,
	offer *Offer,
	limitCounts map[string]map[string]int64,
	numTasksPerOffer int64) int64 {

	// check dedicated attributes vs. constraints
	if !isDedicated(offer, taskConfig.Job.Role, taskConfig.Constraints) {
		return 0
	}

	limitConstraints := []*aurora.Constraint{}

	for _, c := range taskConfig.Constraints {
		// look for corresponding attribute
		attFound := false
		for _, a := range offer.Attributes {
			if a.Name == c.Name {
				attFound = true
			}
		}

		// constraint not found in offer's attributes
		if !attFound {
			return 0
		}

		if c.Constraint.Value != nil && !valueConstraint(offer, c) {
			// value constraint is not satisfied
			return 0
		} else if c.Constraint.Limit != nil {
			limitConstraints = append(limitConstraints, c)
			limit := limitConstraint(offer, c, limitCounts)

			if numTasksPerOffer > limit && limit >= 0 {
				numTasksPerOffer = limit
			}
		}
	}

	// update limitCounts
	for _, c := range limitConstraints {
		for _, a := range offer.Attributes {
			if a.Name == c.Name {
				limitCounts[a.Name][a.Text.Value] += numTasksPerOffer
			}
		}
	}

	return numTasksPerOffer
}

func isDedicated(offer *Offer, role string, constraints []*aurora.Constraint) bool {
	// get all dedicated attributes of an offer
	dedicatedAtts := map[string]bool{}
	for _, a := range offer.Attributes {
		if a.Name == "dedicated" {
			dedicatedAtts[a.Text.Value] = true
		}
	}

	if len(dedicatedAtts) == 0 {
		return true
	}

	// check if constraints are matching dedicated attributes
	matched := false
	for _, c := range constraints {
		if c.Name == "dedicated" && c.Constraint.Value != nil {
			found := false

			for _, v := range c.Constraint.Value.Values {
				if dedicatedAtts[v] && strings.HasPrefix(v, fmt.Sprintf("%s/", role)) {
					found = true
					break
				}
			}

			if found {
				matched = true
			} else {
				return false
			}
		}
	}

	return matched
}

// valueConstraint checks Value Contraints of task if the are matched by the offer.
// more details can be found here https://aurora.apache.org/documentation/latest/features/constraints/
func valueConstraint(offer *Offer, constraint *aurora.Constraint) bool {
	matched := false

	for _, a := range offer.Attributes {
		if a.Name == constraint.Name {
			for _, v := range constraint.Constraint.Value.Values {
				matched = (a.Text.Value == v && !constraint.Constraint.Value.Negated) ||
					(a.Text.Value != v && constraint.Constraint.Value.Negated)

				if matched {
					break
				}
			}

			if matched {
				break
			}
		}
	}

	return matched
}

// limitConstraint limits the number of pods on a group which has the same attribute.
// more details can be found here https://aurora.apache.org/documentation/latest/features/constraints/
func limitConstraint(offer *Offer, constraint *aurora.Constraint, limitCounts map[string]map[string]int64) int64 {
	limit := int64(-1)
	for _, a := range offer.Attributes {
		// limit constraint found
		if a.Name == constraint.Name {
			curr := limitCounts[a.Name][a.Text.Value]
			currLimit := int64(constraint.Constraint.Limit.Limit)

			if curr >= currLimit {
				return 0
			}

			if currLimit-curr < limit || limit < 0 {
				limit = currLimit - curr
			}
		}
	}

	return limit
}
