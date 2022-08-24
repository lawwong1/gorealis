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
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
)

type Endpoint struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type ServiceInstance struct {
	Service             Endpoint            `json:"serviceEndpoint"`
	AdditionalEndpoints map[string]Endpoint `json:"additionalEndpoints"`
	Status              string              `json:"status"`
}

type MesosAddress struct {
	Hostname string `json:"hostname"`
	IP       string `json:"ip"`
	Port     uint16 `json:"port"`
}

// MesosInstance is defined for mesos json stored in ZK.
type MesosInstance struct {
	Address MesosAddress `json:"address"`
	Version string       `json:"version"`
}

type zkConfig struct {
	endpoints []string
	path      string
	backoff   Backoff
	timeout   time.Duration
	logger    Logger
}

type ZKOpt func(z *zkConfig)

func ZKEndpoints(endpoints ...string) ZKOpt {
	return func(z *zkConfig) {
		z.endpoints = endpoints
	}
}

func ZKPath(path string) ZKOpt {
	return func(z *zkConfig) {
		z.path = path
	}
}

func ZKBackoff(b Backoff) ZKOpt {
	return func(z *zkConfig) {
		z.backoff = b
	}
}

func ZKTimeout(d time.Duration) ZKOpt {
	return func(z *zkConfig) {
		z.timeout = d
	}
}

func ZKLogger(l Logger) ZKOpt {
	return func(z *zkConfig) {
		z.logger = l
	}
}

// Retrieves current Aurora leader from ZK.
func LeaderFromZK(cluster Cluster) (string, error) {
	return LeaderFromZKOpts(ZKEndpoints(strings.Split(cluster.ZK, ",")...), ZKPath(cluster.SchedZKPath))
}

// Retrieves current Aurora leader from ZK with a custom configuration.
func LeaderFromZKOpts(options ...ZKOpt) (string, error) {
	var leaderURL string

	// Load the default configuration for Zookeeper followed by overriding values with those provided by the caller.
	config := &zkConfig{backoff: defaultBackoff, timeout: time.Second * 10, logger: NoopLogger{}}
	for _, opt := range options {
		opt(config)
	}

	if len(config.endpoints) == 0 {
		return "", errors.New("no Zookeeper endpoints supplied")
	}

	if config.path == "" {
		return "", errors.New("no Zookeeper path supplied")
	}

	// Create a closure that allows us to use the ExponentialBackoff function.
	retryErr := ExponentialBackoff(config.backoff, config.logger, func() (bool, error) {

		c, _, err := zk.Connect(config.endpoints, config.timeout, func(c *zk.Conn) { c.SetLogger(config.logger) })
		if err != nil {
			return false, NewTemporaryError(errors.Wrap(err, "Failed to connect to Zookeeper"))
		}

		defer c.Close()

		// Open up descriptor for the ZK path given
		children, _, _, err := c.ChildrenW(config.path)
		if err != nil {

			// Sentinel error check as there is no other way to check.
			if err == zk.ErrInvalidPath {
				return false, errors.Wrapf(err, "path %s is an invalid Zookeeper path", config.path)
			}

			return false,
				NewTemporaryError(errors.Wrapf(err, "path %s doesn't exist on Zookeeper ", config.path))
		}

		// Search for the leader through all the children in the given path
		serviceInst := new(ServiceInstance)
		for _, child := range children {

			// Only the leader will start with member_
			if strings.HasPrefix(child, "member_") {

				childPath := config.path + "/" + child
				data, _, err := c.Get(childPath)
				if err != nil {
					if err == zk.ErrInvalidPath {
						return false, errors.Wrapf(err, "path %s is an invalid Zookeeper path", childPath)
					}

					return false, NewTemporaryError(errors.Wrap(err, "error fetching contents of leader"))
				}

				err = json.Unmarshal([]byte(data), serviceInst)
				if err != nil {
					return false,
						NewTemporaryError(errors.Wrap(err, "unable to unmarshal contents of leader"))
				}

				// Should only be one endpoint.
				// This should never be encountered as it would indicate Aurora
				// writing bad info into Zookeeper but is kept here as a safety net.
				if len(serviceInst.AdditionalEndpoints) > 1 {
					return false,
						NewTemporaryError(
							errors.New("ambiguous endpoints in json blob, Aurora wrote bad info to ZK"))
				}

				var scheme, host, port string
				for k, v := range serviceInst.AdditionalEndpoints {
					scheme = k
					host = v.Host
					port = strconv.Itoa(v.Port)
				}

				leaderURL = scheme + "://" + host + ":" + port
				return true, nil
			}
		}

		// Leader data might not be available yet, try to fetch again.
		return false, NewTemporaryError(errors.New("no leader found"))
	})

	if retryErr != nil {
		config.logger.Printf("Failed to determine leader after %v attempts", config.backoff.Steps)
		return "", retryErr
	}

	return leaderURL, nil
}

// Retrieves current mesos leader from ZK with a custom configuration.
func MesosFromZKOpts(options ...ZKOpt) (string, error) {
	var mesosURL string

	// Load the default configuration for Zookeeper followed by overriding values with those provided by the caller.
	config := &zkConfig{backoff: defaultBackoff, timeout: time.Second * 10, logger: NoopLogger{}}
	for _, opt := range options {
		opt(config)
	}

	if len(config.endpoints) == 0 {
		return "", errors.New("no Zookeeper endpoints supplied")
	}

	if config.path == "" {
		return "", errors.New("no Zookeeper path supplied")
	}

	// Create a closure that allows us to use the ExponentialBackoff function.
	retryErr := ExponentialBackoff(config.backoff, config.logger, func() (bool, error) {

		c, _, err := zk.Connect(config.endpoints, config.timeout, func(c *zk.Conn) { c.SetLogger(config.logger) })
		if err != nil {
			return false, NewTemporaryError(errors.Wrap(err, "Failed to connect to Zookeeper"))
		}

		defer c.Close()

		// Open up descriptor for the ZK path given
		children, _, _, err := c.ChildrenW(config.path)
		if err != nil {

			// Sentinel error check as there is no other way to check.
			if err == zk.ErrInvalidPath {
				return false, errors.Wrapf(err, "path %s is an invalid Zookeeper path", config.path)
			}

			return false,
				NewTemporaryError(errors.Wrapf(err, "path %s doesn't exist on Zookeeper ", config.path))
		}

		// Search for the leader through all the children in the given path
		minScore := math.MaxInt64
		var mesosInstance MesosInstance
		for _, child := range children {
			// Only the leader will start with json.info_
			if strings.HasPrefix(child, "json.info_") {
				strs := strings.Split(child, "_")
				if len(strs) < 2 {
					config.logger.Printf("Zk node %v/%v's name is malformed.", config.path, child)
					continue
				}
				score, err := strconv.Atoi(strs[1])
				if err != nil {
					return false, NewTemporaryError(errors.Wrap(err, "unable to read the zk node for Mesos."))
				}

				// get the leader from the child with the smallest score.
				if score < minScore {
					minScore = score
					childPath := config.path + "/" + child
					data, _, err := c.Get(childPath)
					if err != nil {
						if err == zk.ErrInvalidPath {
							return false, errors.Wrapf(err, "path %s is an invalid Zookeeper path", childPath)
						}

						return false, NewTemporaryError(errors.Wrap(err, "error fetching contents of leader"))
					}

					err = json.Unmarshal([]byte(data), &mesosInstance)
					if err != nil {
						config.logger.Printf("%s", err)
						return false,
							NewTemporaryError(errors.Wrap(err, "unable to unmarshal contents of leader"))
					}

					mesosURL = mesosInstance.Address.IP + ":" + strconv.Itoa(int(mesosInstance.Address.Port))
				}
			}
		}
		if len(mesosURL) > 0 {
			return true, nil
		}

		// Leader data might not be available yet, try to fetch again.
		return false, NewTemporaryError(errors.New("no leader found"))
	})

	if retryErr != nil {
		config.logger.Printf("Failed to determine leader after %v attempts", config.backoff.Steps)
		return "", retryErr
	}

	return mesosURL, nil
}

// Retrieves current Aurora master nodes from ZK.
func MasterNodesFromZK(cluster Cluster) (map[string][]string, error) {
	return MasterNodesFromZKOpts(ZKEndpoints(strings.Split(cluster.ZK, ",")...), ZKPath(cluster.SchedZKPath))
}

// Retrieves current Mesos master nodes/leader from ZK with a custom configuration.
func MasterNodesFromZKOpts(options ...ZKOpt) (map[string][]string, error) {
	result := make(map[string][]string)

	// Load the default configuration for Zookeeper followed by overriding values with those provided by the caller.
	config := &zkConfig{backoff: defaultBackoff, timeout: time.Second * 10, logger: NoopLogger{}}
	for _, opt := range options {
		opt(config)
	}

	if len(config.endpoints) == 0 {
		return nil, errors.New("no Zookeeper endpoints supplied")
	}

	if config.path == "" {
		return nil, errors.New("no Zookeeper path supplied")
	}

	// Create a closure that allows us to use the ExponentialBackoff function.
	retryErr := ExponentialBackoff(config.backoff, config.logger, func() (bool, error) {

		c, _, err := zk.Connect(config.endpoints, config.timeout, func(c *zk.Conn) { c.SetLogger(config.logger) })
		if err != nil {
			return false, NewTemporaryError(errors.Wrap(err, "Failed to connect to Zookeeper"))
		}

		defer c.Close()

		// Open up descriptor for the ZK path given
		children, _, _, err := c.ChildrenW(config.path)
		if err != nil {

			// Sentinel error check as there is no other way to check.
			if err == zk.ErrInvalidPath {
				return false, errors.Wrapf(err, "path %s is an invalid Zookeeper path", config.path)
			}

			return false,
				NewTemporaryError(errors.Wrapf(err, "path %s doesn't exist on Zookeeper ", config.path))
		}

		// Get all the master nodes through all the children in the given path
		serviceInst := new(ServiceInstance)
		var hosts []string
		for _, child := range children {
			childPath := config.path + "/" + child
			data, _, err := c.Get(childPath)
			if err != nil {
				if err == zk.ErrInvalidPath {
					return false, errors.Wrapf(err, "path %s is an invalid Zookeeper path", childPath)
				}

				return false, NewTemporaryError(errors.Wrap(err, "error fetching contents of leader"))
			}
			// Only leader is in json format. Have to parse data differently between member_ and not member_
			if strings.HasPrefix(child, "member_") {
				err = json.Unmarshal([]byte(data), &serviceInst)
				if err != nil {
					return false,
						NewTemporaryError(errors.Wrap(err, "unable to unmarshal contents of leader"))
				}
				// Should only be one endpoint.
				// This should never be encountered as it would indicate Aurora
				// writing bad info into Zookeeper but is kept here as a safety net.
				if len(serviceInst.AdditionalEndpoints) > 1 {
					return false,
						NewTemporaryError(
							errors.New("ambiguous endpoints in json blob, Aurora wrote bad info to ZK"))
				}

				for _, v := range serviceInst.AdditionalEndpoints {
					result["leader"] = append(result["leader"], v.Host)
				}
			} else {
				// data is not in a json format
				hosts = append(hosts, string(data))
			}
		}
		result["masterNodes"] = hosts

		// Master nodes data might not be available yet, try to fetch again.
		if len(result["masterNodes"]) == 0 {
			return false, NewTemporaryError(errors.New("no master nodes found"))
		}
		return true, nil
	})

	if retryErr != nil {
		config.logger.Printf("Failed to get master nodes after %v attempts", config.backoff.Steps)
		return nil, retryErr
	}

	return result, nil
}

// Retrieves current Mesos Aurora master nodes from ZK.
func MesosMasterNodesFromZK(cluster Cluster) (map[string][]string, error) {
	return MesosMasterNodesFromZKOpts(ZKEndpoints(strings.Split(cluster.ZK, ",")...), ZKPath(cluster.MesosZKPath))
}

// Retrieves current mesos master nodes/leader from ZK with a custom configuration.
func MesosMasterNodesFromZKOpts(options ...ZKOpt) (map[string][]string, error) {
	result := make(map[string][]string)

	// Load the default configuration for Zookeeper followed by overriding values with those provided by the caller.]
	config := &zkConfig{backoff: defaultBackoff, timeout: time.Second * 10, logger: NoopLogger{}}
	for _, opt := range options {
		opt(config)
	}

	if len(config.endpoints) == 0 {
		return nil, errors.New("no Zookeeper endpoints supplied")
	}

	if config.path == "" {
		return nil, errors.New("no Zookeeper path supplied")
	}

	// Create a closure that allows us to use the ExponentialBackoff function.
	retryErr := ExponentialBackoff(config.backoff, config.logger, func() (bool, error) {

		c, _, err := zk.Connect(config.endpoints, config.timeout, func(c *zk.Conn) { c.SetLogger(config.logger) })
		if err != nil {
			return false, NewTemporaryError(errors.Wrap(err, "Failed to connect to Zookeeper"))
		}

		defer c.Close()

		// Open up descriptor for the ZK path given
		children, _, _, err := c.ChildrenW(config.path)
		if err != nil {

			// Sentinel error check as there is no other way to check.
			if err == zk.ErrInvalidPath {
				return false, errors.Wrapf(err, "path %s is an invalid Zookeeper path", config.path)
			}

			return false,
				NewTemporaryError(errors.Wrapf(err, "path %s doesn't exist on Zookeeper ", config.path))
		}

		// Get all the master nodes through all the children in the given path
		minScore := math.MaxInt64
		var mesosInstance MesosInstance
		var hosts []string
		for _, child := range children {
			// Only the master nodes will start with json.info_
			if strings.HasPrefix(child, "json.info_") {
				strs := strings.Split(child, "_")
				if len(strs) < 2 {
					config.logger.Printf("Zk node %v/%v's name is malformed.", config.path, child)
					continue
				}
				score, err := strconv.Atoi(strs[1])
				if err != nil {
					return false, NewTemporaryError(errors.Wrap(err, "unable to read the zk node for Mesos."))
				}

				childPath := config.path + "/" + child
				data, _, err := c.Get(childPath)
				if err != nil {
					if err == zk.ErrInvalidPath {
						return false, errors.Wrapf(err, "path %s is an invalid Zookeeper path", childPath)
					}

					return false, NewTemporaryError(errors.Wrap(err, "error fetching contents of leader"))
				}

				err = json.Unmarshal([]byte(data), &mesosInstance)
				if err != nil {
					config.logger.Printf("%s", err)
					return false,
						NewTemporaryError(errors.Wrap(err, "unable to unmarshal contents of leader"))
				}
				// Combine all master nodes into comma-separated
				// Return hostname instead of ip to be consistent with aurora master nodes
				hosts = append(hosts, mesosInstance.Address.Hostname)
				// get the leader from the child with the smallest score.
				if score < minScore {
					minScore = score
					result["leader"] = append(result["leader"], mesosInstance.Address.Hostname)
				}
			}
		}
		result["masterNodes"] = hosts
		// Master nodes data might not be available yet, try to fetch again.
		if len(result["masterNodes"]) == 0 {
			return false, NewTemporaryError(errors.New("no mesos master nodes found"))
		}
		return true, nil
	})

	if retryErr != nil {
		config.logger.Printf("Failed to get mesos master nodes after %v attempts", config.backoff.Steps)
		return nil, retryErr
	}

	return result, nil
}
