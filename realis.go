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

// Package realis provides the ability to use Thrift API to communicate with Apache Aurora.
package realis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/aurora-scheduler/gorealis/v2/gen-go/apache/aurora"
	"github.com/aurora-scheduler/gorealis/v2/response"
	"github.com/pkg/errors"
)

const VERSION = "2.28.0"

type Client struct {
	config         *clientConfig
	client         *aurora.AuroraSchedulerManagerClient
	readonlyClient *aurora.ReadOnlySchedulerClient
	adminClient    *aurora.AuroraAdminClient
	logger         LevelLogger
	lock           *sync.Mutex
	debug          bool
	transport      thrift.TTransport
}

func newTJSONTransport(url string, timeout time.Duration, config *clientConfig) (thrift.TTransport, error) {
	trans, err := defaultTTransport(url, timeout, config)
	if err != nil {
		return nil, errors.Wrap(err, "error creating realis")
	}

	httpTrans, ok := (trans).(*thrift.THttpClient)
	if !ok {
		return nil, errors.Wrap(err, "transport does not contain a thrift client")
	}

	httpTrans.SetHeader("Content-Type", "application/x-thrift")
	httpTrans.SetHeader("User-Agent", "gorealis v"+VERSION)
	return trans, err
}

func newTBinTransport(url string, timeout time.Duration, config *clientConfig) (thrift.TTransport, error) {
	trans, err := defaultTTransport(url, timeout, config)
	if err != nil {
		return nil, errors.Wrap(err, "error creating realis")
	}

	httpTrans, ok := (trans).(*thrift.THttpClient)
	if !ok {
		return nil, errors.Wrap(err, "transport does not contain a thrift client")
	}

	httpTrans.DelHeader("Content-Type") // Workaround for using thrift HttpPostClient
	httpTrans.SetHeader("Accept", "application/vnd.apache.thrift.binary")
	httpTrans.SetHeader("Content-Type", "application/vnd.apache.thrift.binary")
	httpTrans.SetHeader("User-Agent", "gorealis v"+VERSION)

	return trans, err
}

// This client implementation uses a retry mechanism for all Thrift Calls.
// It will retry all calls which result in a temporary failure as well as calls that fail due to an EOF
// being returned by the http client. Most permanent failures are now being caught by the thriftCallWithRetries
// function and not being retried but there may be corner cases not yet handled.
func NewClient(options ...ClientOption) (*Client, error) {
	config := &clientConfig{}

	// Default configs
	config.timeout = 10 * time.Second
	config.backoff = defaultBackoff
	config.logger = &LevelLogger{Logger: log.New(os.Stdout, "realis: ", log.Ltime|log.Ldate|log.LUTC)}

	// Save options to recreate client if a connection error happens
	config.options = options

	// Override default configs where necessary
	for _, opt := range options {
		opt(config)
	}

	// TODO(rdelvalle): Move this logic to it's own function to make initialization code easier to read.

	// Set a sane logger based upon configuration passed by the user
	if config.logger == nil {
		if config.debug || config.trace {
			config.logger = &LevelLogger{Logger: log.New(os.Stdout, "realis: ", log.Ltime|log.Ldate|log.LUTC)}
		} else {
			config.logger = &LevelLogger{Logger: NoopLogger{}}
		}
	}

	// Note, by this point, a LevelLogger should have been created.
	config.logger.EnableDebug(config.debug)
	config.logger.EnableTrace(config.trace)

	config.logger.DebugPrintln("Number of options applied to clientConfig: ", len(options))

	var url string
	var err error

	// Find the leader using custom Zookeeper options if options are provided
	if config.zkOptions != nil {
		url, err = LeaderFromZKOpts(config.zkOptions...)
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "unable to determine leader from zookeeper"))
		}
		config.logger.Println("Scheduler URL from ZK: ", url)
	} else if config.cluster != nil {
		// Determine how to get information to connect to the scheduler.
		// Prioritize getting leader from ZK over using a direct URL.
		url, err = LeaderFromZK(*config.cluster)
		// If ZK is configured, throw an error if the leader is unable to be determined
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "unable to determine leader from zookeeper"))
		}
		config.logger.Println("Scheduler URL from ZK: ", url)
	} else if config.url != "" {
		url = config.url
		config.logger.Println("Scheduler URL: ", url)
	} else {
		return nil, errors.New("incomplete Options -- url, cluster.json, or Zookeeper address required")
	}

	config.url = url

	url, err = validateAuroraAddress(url)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create realis object, invalid url")
	}

	switch config.transportProtocol {
	case binaryProtocol:
		trans, err := newTBinTransport(url, config.timeout, config)
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "error creating realis"))
		}
		config.transport = trans
		config.protoFactory = thrift.NewTBinaryProtocolFactoryDefault()
	case jsonProtocol:
		fallthrough
	default:
		trans, err := newTJSONTransport(url, config.timeout, config)
		if err != nil {
			return nil, NewTemporaryError(errors.Wrap(err, "error creating realis"))
		}
		config.transport = trans
		config.protoFactory = thrift.NewTJSONProtocolFactory()
	}

	config.logger.Printf("gorealis clientConfig url: %+v\n", url)

	// Adding Basic Authentication.
	if config.username != "" && config.password != "" {
		httpTrans, ok := (config.transport).(*thrift.THttpClient)
		if !ok {
			return nil, errors.New("transport provided does not contain an THttpClient")
		}
		httpTrans.SetHeader("Authorization", "Basic "+basicAuth(config.username, config.password))
	}

	return &Client{
		config:         config,
		client:         aurora.NewAuroraSchedulerManagerClientFactory(config.transport, config.protoFactory),
		readonlyClient: aurora.NewReadOnlySchedulerClientFactory(config.transport, config.protoFactory),
		adminClient:    aurora.NewAuroraAdminClientFactory(config.transport, config.protoFactory),
		// We initialize logger this way to allow any logger which satisfies the Logger interface
		logger:    LevelLogger{Logger: config.logger, debug: config.debug, trace: config.trace},
		lock:      &sync.Mutex{},
		transport: config.transport,
	}, nil
}

func GetCerts(certPath string) (*x509.CertPool, error) {
	globalRootCAs := x509.NewCertPool()
	caFiles, err := ioutil.ReadDir(certPath)
	if err != nil {
		return nil, err
	}
	for _, cert := range caFiles {
		caPathFile := filepath.Join(certPath, cert.Name())
		caCert, err := ioutil.ReadFile(caPathFile)
		if err != nil {
			return nil, err
		}
		globalRootCAs.AppendCertsFromPEM(caCert)
	}
	return globalRootCAs, nil
}

// Creates a default Thrift Transport object for communications in gorealis using an HTTP Post Client
func defaultTTransport(url string, timeout time.Duration, config *clientConfig) (thrift.TTransport, error) {
	var transport http.Transport

	if config != nil {
		tlsConfig := &tls.Config{}
		if config.insecureSkipVerify {
			tlsConfig.InsecureSkipVerify = true
		}
		if config.certsPath != "" {
			rootCAs, err := GetCerts(config.certsPath)
			if err != nil {
				config.logger.Println("error occurred couldn't fetch certs")
				return nil, err
			}
			tlsConfig.RootCAs = rootCAs
		}
		if config.clientKey != "" && config.clientCert == "" {
			return nil, fmt.Errorf("have to provide both client key,cert. Only client key provided ")
		}
		if config.clientKey == "" && config.clientCert != "" {
			return nil, fmt.Errorf("have to provide both client key,cert. Only client cert provided ")
		}
		if config.clientKey != "" && config.clientCert != "" {
			cert, err := tls.LoadX509KeyPair(config.clientCert, config.clientKey)
			if err != nil {
				config.logger.Println("error occurred loading client certs and keys")
				return nil, err
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		transport.TLSClientConfig = tlsConfig
	}

	trans, err := thrift.NewTHttpClientWithOptions(url,
		thrift.THttpClientOptions{
			Client: &http.Client{
				Timeout:   timeout,
				Transport: &transport,
			},
		})

	if err != nil {
		return nil, errors.Wrap(err, "error creating transport")
	}

	if err := trans.Open(); err != nil {
		return nil, errors.Wrapf(err, "error opening connection to %s", url)
	}

	return trans, nil
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func (c *Client) ReestablishConn() error {
	// Close existing connection
	c.logger.Println("Re-establishing Connection to Aurora")
	c.Close()

	c.lock.Lock()
	defer c.lock.Unlock()

	// Recreate connection from scratch using original options
	newRealis, err := NewClient(c.config.options...)
	if err != nil {
		// This could be a temporary network hiccup
		return NewTemporaryError(err)
	}

	// If we are able to successfully re-connect, make receiver
	// point to newly established connections.
	c.config = newRealis.config
	c.client = newRealis.client
	c.readonlyClient = newRealis.readonlyClient
	c.adminClient = newRealis.adminClient
	c.logger = newRealis.logger

	return nil
}

// Releases resources associated with the realis client.
func (c *Client) Close() {

	c.lock.Lock()
	defer c.lock.Unlock()

	c.transport.Close()
}

// Uses predefined set of states to retrieve a set of active jobs in Apache Aurora.
func (c *Client) GetInstanceIds(key aurora.JobKey, states []aurora.ScheduleStatus) ([]int32, error) {
	taskQ := &aurora.TaskQuery{
		Role:        &key.Role,
		Environment: &key.Environment,
		JobName:     &key.Name,
		Statuses:    states,
	}

	c.logger.DebugPrintf("GetInstanceIds Thrift Payload: %+v\n", taskQ)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.GetTasksWithoutConfigs(context.TODO(), taskQ)
	},
		nil,
	)

	// If we encountered an error we couldn't recover from by retrying, return an error to the user
	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error querying Aurora Scheduler for active IDs")
	}

	// Construct instance id map to stay in line with thrift's representation of sets
	tasks := response.ScheduleStatusResult(resp).GetTasks()
	jobInstanceIds := make([]int32, 0, len(tasks))
	for _, task := range tasks {
		jobInstanceIds = append(jobInstanceIds, task.GetAssignedTask().GetInstanceId())
	}
	return jobInstanceIds, nil

}

func (c *Client) GetJobUpdateSummaries(jobUpdateQuery *aurora.JobUpdateQuery) (*aurora.GetJobUpdateSummariesResult_, error) {
	c.logger.DebugPrintf("GetJobUpdateSummaries Thrift Payload: %+v\n", jobUpdateQuery)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.readonlyClient.GetJobUpdateSummaries(context.TODO(), jobUpdateQuery)
	},
		nil,
	)

	if resp == nil || resp.GetResult_() == nil || resp.GetResult_().GetGetJobUpdateSummariesResult_() == nil {
		return nil, errors.New("unexpected response from scheduler")
	}
	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error getting job update summaries from Aurora Scheduler")
	}

	return resp.GetResult_().GetGetJobUpdateSummariesResult_(), nil
}

func (c *Client) GetJobSummary(role string) (*aurora.JobSummaryResult_, error) {

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.readonlyClient.GetJobSummary(context.TODO(), role)
	},
		nil,
	)
	if resp == nil || resp.GetResult_() == nil || resp.GetResult_().GetJobSummaryResult_() == nil {
		return nil, errors.New("unexpected response from scheduler")
	}
	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error getting job summaries from Aurora Scheduler")
	}

	return resp.GetResult_().GetJobSummaryResult_(), nil
}

func (c *Client) GetJobs(role string) (*aurora.GetJobsResult_, error) {

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.readonlyClient.GetJobs(context.TODO(), role)
	},
		nil,
	)

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error getting Jobs from Aurora Scheduler")
	}
	if resp == nil || resp.GetResult_() == nil {
		return nil, errors.New("unexpected response from scheduler")
	}

	return resp.GetResult_().GetJobsResult_, nil
}

// Kill specific instances of a job. Returns true, nil if a task was actually killed as a result of this API call.
// Returns false, nil if no tasks were killed as a result of this call but there was no error making the call.
func (c *Client) KillInstances(key aurora.JobKey, instances ...int32) (bool, error) {
	c.logger.DebugPrintf("KillTasks Thrift Payload: %+v %v\n", key, instances)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.KillTasks(context.TODO(), &key, instances, "")
	},
		nil,
	)

	if retryErr != nil {
		return false, errors.Wrap(retryErr, "error sending Kill command to Aurora Scheduler")
	}

	if resp == nil || len(resp.GetDetails()) > 0 {
		c.logger.Println("KillTasks was called but no tasks killed as a result.")
		return false, nil
	}
	return true, nil
}

func (c *Client) RealisConfig() *clientConfig {
	return c.config
}

// Sends a kill message to the scheduler for all active tasks under a job.
func (c *Client) KillJob(key aurora.JobKey) error {

	c.logger.DebugPrintf("KillTasks Thrift Payload: %+v\n", key)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		// Giving the KillTasks thrift call an empty set tells the Aurora scheduler to kill all active shards
		return c.client.KillTasks(context.TODO(), &key, nil, "")
	},
		nil,
	)

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending Kill command to Aurora Scheduler")
	}
	return nil
}

// Sends a create job message to the scheduler with a specific job configuration.
// Although this API is able to create service jobs, it is better to use CreateService instead
// as that API uses the update thrift call which has a few extra features available.
// Use this API to create ad-hoc jobs.
func (c *Client) CreateJob(auroraJob *AuroraJob) error {
	// If no thermos configuration has been set this will result in a NOOP
	err := auroraJob.BuildThermosPayload()

	c.logger.DebugPrintf("CreateJob Thrift Payload: %+v\n", auroraJob.JobConfig())

	if err != nil {
		return errors.Wrap(err, "unable to create Thermos payload")
	}

	// Response is checked by the thrift retry code
	_, retryErr := c.thriftCallWithRetries(
		false,
		func() (*aurora.Response, error) {
			return c.client.CreateJob(context.TODO(), auroraJob.JobConfig())
		},
		// On a client timeout, attempt to verify that payload made to the Scheduler by
		// trying to get the config summary for the job key
		func() (*aurora.Response, bool) {
			exists, err := c.JobExists(auroraJob.JobKey())
			if err != nil {
				c.logger.Print("verification failed ", err)
			}

			if exists {
				return &aurora.Response{ResponseCode: aurora.ResponseCode_OK}, true
			}

			return nil, false
		},
	)

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending Create command to Aurora Scheduler")
	}

	return nil
}

// This API uses an update thrift call to create the services giving a few more robust features.
func (c *Client) CreateService(update *JobUpdate) (*aurora.StartJobUpdateResult_, error) {
	updateResult, err := c.StartJobUpdate(update, "")
	if err != nil {
		return nil, errors.Wrap(err, "unable to create service")
	}

	return updateResult, err
}

func (c *Client) ScheduleCronJob(auroraJob *AuroraJob) error {
	// If no thermos configuration has been set this will result in a NOOP
	err := auroraJob.BuildThermosPayload()

	c.logger.DebugPrintf("ScheduleCronJob Thrift Payload: %+v\n", auroraJob.JobConfig())

	if err != nil {
		return errors.Wrap(err, "Unable to create Thermos payload")
	}

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.ScheduleCronJob(context.TODO(), auroraJob.JobConfig())
	},
		nil,
	)

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending Cron AuroraJob Schedule message to Aurora Scheduler")
	}
	return nil
}

func (c *Client) DescheduleCronJob(key aurora.JobKey) error {

	c.logger.DebugPrintf("DescheduleCronJob Thrift Payload: %+v\n", key)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.DescheduleCronJob(context.TODO(), &key)
	},
		nil,
	)

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending Cron AuroraJob De-schedule message to Aurora Scheduler")

	}
	return nil

}

func (c *Client) StartCronJob(key aurora.JobKey) error {

	c.logger.DebugPrintf("StartCronJob Thrift Payload: %+v\n", key)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.StartCronJob(context.TODO(), &key)
	},
		nil,
	)

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending Start Cron AuroraJob  message to Aurora Scheduler")
	}
	return nil

}

// Restarts specific instances specified
func (c *Client) RestartInstances(key aurora.JobKey, instances ...int32) error {
	c.logger.DebugPrintf("RestartShards Thrift Payload: %+v %v\n", key, instances)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.RestartShards(context.TODO(), &key, instances)
	},
		nil,
	)

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending Restart command to Aurora Scheduler")
	}
	return nil
}

// Restarts all active tasks under a job configuration.
func (c *Client) RestartJob(key aurora.JobKey) error {

	instanceIds, err1 := c.GetInstanceIds(key, aurora.ACTIVE_STATES)
	if err1 != nil {
		return errors.Wrap(err1, "could not retrieve relevant task instance IDs")
	}

	c.logger.DebugPrintf("RestartShards Thrift Payload: %+v %v\n", key, instanceIds)

	if len(instanceIds) > 0 {
		_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
			return c.client.RestartShards(context.TODO(), &key, instanceIds)
		},
			nil,
		)

		if retryErr != nil {
			return errors.Wrap(retryErr, "error sending Restart command to Aurora Scheduler")
		}

		return nil
	}
	return errors.New("no tasks in the Active state")
}

// Update all tasks under a job configuration. Currently gorealis doesn't support for canary deployments.
func (c *Client) StartJobUpdate(updateJob *JobUpdate, message string) (*aurora.StartJobUpdateResult_, error) {

	if err := updateJob.BuildThermosPayload(); err != nil {
		return nil, errors.New("unable to generate the proper Thermos executor payload")
	}

	c.logger.DebugPrintf("StartJobUpdate Thrift Payload: %+v %v\n", updateJob, message)

	resp, retryErr := c.thriftCallWithRetries(false,
		func() (*aurora.Response, error) {
			return c.client.StartJobUpdate(context.TODO(), updateJob.request, message)
		},
		func() (*aurora.Response, bool) {
			key := updateJob.JobKey()
			summariesResp, err := c.readonlyClient.GetJobUpdateSummaries(
				context.TODO(),
				&aurora.JobUpdateQuery{
					JobKey:         &key,
					UpdateStatuses: aurora.ACTIVE_JOB_UPDATE_STATES,
					Limit:          1,
				})

			if err != nil {
				c.logger.Print("verification failed ", err)
				return nil, false
			}

			summaries := response.JobUpdateSummaries(summariesResp)
			if len(summaries) == 0 {
				return nil, false
			}

			return &aurora.Response{
				ResponseCode: aurora.ResponseCode_OK,
				Result_: &aurora.Result_{
					StartJobUpdateResult_: &aurora.StartJobUpdateResult_{
						UpdateSummary: summaries[0],
						Key:           summaries[0].Key,
					},
				},
			}, true
		},
	)

	if retryErr != nil {
		// A timeout took place when attempting this call, attempt to recover
		if IsTimeout(retryErr) {
			return nil, retryErr
		}

		return nil, errors.Wrap(retryErr, "error sending StartJobUpdate command to Aurora Scheduler")
	}
	if resp == nil || resp.GetResult_() == nil || resp.GetResult_().GetStartJobUpdateResult_() == nil {
		return nil, errors.New("unexpected response from scheduler")
	}
	return resp.GetResult_().GetStartJobUpdateResult_(), nil
}

// AbortJobUpdate terminates a job update in the scheduler.
// It requires the updateId which can be obtained on the Aurora web UI.
// This API is meant to be synchronous. It will attempt to wait until the update transitions to the aborted state.
// However, if the job update does not transition to the ABORT state an error will be returned.
func (c *Client) AbortJobUpdate(updateKey aurora.JobUpdateKey, message string) error {

	c.logger.DebugPrintf("AbortJobUpdate Thrift Payload: %+v %v\n", updateKey, message)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.AbortJobUpdate(context.TODO(), &updateKey, message)
	},
		nil,
	)

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending AbortJobUpdate command to Aurora Scheduler")
	}
	// Make this call synchronous by  blocking until it job has successfully transitioned to aborted
	_, err := c.MonitorJobUpdateStatus(
		updateKey,
		[]aurora.JobUpdateStatus{aurora.JobUpdateStatus_ABORTED},
		time.Second*5,
		time.Minute)
	return err
}

// Pause AuroraJob Update. UpdateID is returned from StartJobUpdate or the Aurora web UI.
func (c *Client) PauseJobUpdate(updateKey *aurora.JobUpdateKey, message string) error {

	c.logger.DebugPrintf("PauseJobUpdate Thrift Payload: %+v %v\n", updateKey, message)
	// Thrift uses pointers for optional fields when generating Go code. To guarantee
	// immutability of the JobUpdateKey, perform a deep copy and store it locally.
	updateKeyLocal := &aurora.JobUpdateKey{
		Job: &aurora.JobKey{
			Role:        updateKey.Job.GetRole(),
			Environment: updateKey.Job.GetEnvironment(),
			Name:        updateKey.Job.GetName(),
		},
		ID: updateKey.GetID(),
	}

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.PauseJobUpdate(nil, updateKeyLocal, message)
	},
		nil,
	)

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending PauseJobUpdate command to Aurora Scheduler")
	}

	// Make this call synchronous by  blocking until it job has successfully transitioned to aborted
	_, err := c.MonitorJobUpdateStatus(*updateKeyLocal,
		[]aurora.JobUpdateStatus{aurora.JobUpdateStatus_ABORTED},
		time.Second*5,
		time.Minute)

	return err
}

// Resume Paused AuroraJob Update. UpdateID is returned from StartJobUpdate or the Aurora web UI.
func (c *Client) ResumeJobUpdate(updateKey aurora.JobUpdateKey, message string) error {

	c.logger.DebugPrintf("ResumeJobUpdate Thrift Payload: %+v %v\n", updateKey, message)
	updateKey.Job = &aurora.JobKey{
		Role:        updateKey.Job.Role,
		Environment: updateKey.Job.Environment,
		Name:        updateKey.Job.Name,
	}

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.ResumeJobUpdate(context.TODO(), &updateKey, message)
	},
		nil,
	)

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending ResumeJobUpdate command to Aurora Scheduler")
	}

	return nil
}

// Pulse AuroraJob Update on Aurora. UpdateID is returned from StartJobUpdate or the Aurora web UI.
func (c *Client) PulseJobUpdate(updateKey aurora.JobUpdateKey) (aurora.JobUpdatePulseStatus, error) {

	c.logger.DebugPrintf("PulseJobUpdate Thrift Payload: %+v\n", updateKey)
	updateKey.Job = &aurora.JobKey{
		Role:        updateKey.Job.Role,
		Environment: updateKey.Job.Environment,
		Name:        updateKey.Job.Name,
	}

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.PulseJobUpdate(context.TODO(), &updateKey)
	},
		nil,
	)

	if retryErr != nil {
		return aurora.JobUpdatePulseStatus(0), errors.Wrap(retryErr, "error sending PulseJobUpdate command to Aurora Scheduler")
	}

	if resp == nil || resp.GetResult_() == nil || resp.GetResult_().GetPulseJobUpdateResult_() == nil {
		return aurora.JobUpdatePulseStatus(0), errors.New("unexpected response from scheduler")
	}

	return resp.GetResult_().GetPulseJobUpdateResult_().GetStatus(), nil
}

// Scale up the number of instances under a job configuration using the configuration for specific
// instance to scale up.
func (c *Client) AddInstances(instKey aurora.InstanceKey, count int32) error {

	c.logger.DebugPrintf("AddInstances Thrift Payload: %+v %v\n", instKey, count)

	instKey.JobKey = &aurora.JobKey{
		Role:        instKey.JobKey.Name,
		Environment: instKey.JobKey.Environment,
		Name:        instKey.JobKey.Name,
	}

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.AddInstances(context.TODO(), &instKey, count)
	},
		nil,
	)

	if retryErr != nil {
		return errors.Wrap(retryErr, "error sending AddInstances command to Aurora Scheduler")
	}
	return nil

}

// Scale down the number of instances under a job configuration using the configuration of a specific instance
// Instances with a higher instance ID will be removed first. For example, if our instance ID list is [0,1,2,3]
// and we want to remove 2 instances, 2 and 3 will always be picked.
func (c *Client) RemoveInstances(key aurora.JobKey, count int) error {
	instanceIds, err := c.GetInstanceIds(key, aurora.ACTIVE_STATES)
	if err != nil {
		return errors.Wrap(err, "removeInstances: Could not retrieve relevant instance IDs")
	}
	if len(instanceIds) < count {
		return errors.Errorf("insufficient active instances available for killing: "+
			" Instances to be killed %d Active instances %d", count, len(instanceIds))
	}

	// Sort instanceIds in decreasing order
	sort.Slice(instanceIds, func(i, j int) bool {
		return instanceIds[i] > instanceIds[j]
	})

	// Get the last count instance ids to kill
	instanceIds = instanceIds[:count]
	killed, err := c.KillInstances(key, instanceIds...)

	if !killed {
		return errors.New("flex down was not able to reduce the number of instances running.")
	}

	return nil
}

// Get information about task including a fully hydrated task configuration object
func (c *Client) GetTaskStatus(query *aurora.TaskQuery) ([]*aurora.ScheduledTask, error) {

	c.logger.DebugPrintf("GetTasksStatus Thrift Payload: %+v\n", query)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.GetTasksStatus(context.TODO(), query)
	},
		nil,
	)

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error querying Aurora Scheduler for task status")
	}
	if resp == nil {
		return nil, errors.New("unexpected response from scheduler")
	}

	return response.ScheduleStatusResult(resp).GetTasks(), nil
}

// Get pending reason
func (c *Client) GetPendingReason(query *aurora.TaskQuery) ([]*aurora.PendingReason, error) {

	c.logger.DebugPrintf("GetPendingReason Thrift Payload: %+v\n", query)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.GetPendingReason(context.TODO(), query)
	},
		nil,
	)

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error querying Aurora Scheduler for pending Reasons")
	}

	if resp == nil || resp.GetResult_() == nil || resp.GetResult_().GetGetPendingReasonResult_() == nil {
		return nil, errors.New("unexpected response from scheduler")
	}

	return resp.GetResult_().GetGetPendingReasonResult_().GetReasons(), nil
}

// GetTasksWithoutConfigs gets information about task including without a task configuration object.
// This is a more lightweight version of GetTaskStatus but contains less information as a result.
func (c *Client) GetTasksWithoutConfigs(query *aurora.TaskQuery) ([]*aurora.ScheduledTask, error) {

	c.logger.DebugPrintf("GetTasksWithoutConfigs Thrift Payload: %+v\n", query)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.GetTasksWithoutConfigs(context.TODO(), query)
	},
		nil,
	)

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error querying Aurora Scheduler for task status without configs")
	}

	return response.ScheduleStatusResult(resp).GetTasks(), nil

}

// Get the task configuration from the aurora scheduler for a job
func (c *Client) FetchTaskConfig(instKey aurora.InstanceKey) (*aurora.TaskConfig, error) {

	ids := []int32{instKey.GetInstanceId()}

	taskQ := &aurora.TaskQuery{
		Role:        &instKey.JobKey.Role,
		Environment: &instKey.JobKey.Environment,
		JobName:     &instKey.JobKey.Name,
		InstanceIds: ids,
		Statuses:    aurora.ACTIVE_STATES,
	}

	c.logger.DebugPrintf("GetTasksStatus Thrift Payload: %+v\n", taskQ)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.GetTasksStatus(context.TODO(), taskQ)
	},
		nil,
	)

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "error querying Aurora Scheduler for task configuration")
	}

	tasks := response.ScheduleStatusResult(resp).GetTasks()

	if len(tasks) == 0 {
		return nil, errors.Errorf("instance %d for jobkey %s/%s/%s doesn't exist",
			instKey.InstanceId,
			instKey.JobKey.Environment,
			instKey.JobKey.Role,
			instKey.JobKey.Name)
	}

	// Currently, instance 0 is always picked..
	return tasks[0].AssignedTask.Task, nil
}

func (c *Client) JobUpdateDetails(updateQuery aurora.JobUpdateQuery) ([]*aurora.JobUpdateDetails, error) {

	c.logger.DebugPrintf("GetJobUpdateDetails Thrift Payload: %+v\n", updateQuery)

	resp, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.GetJobUpdateDetails(context.TODO(), &updateQuery)
	},
		nil,
	)

	if retryErr != nil {
		return nil, errors.Wrap(retryErr, "unable to get job update details")
	}

	if resp == nil || resp.GetResult_() == nil || resp.GetResult_().GetGetJobUpdateDetailsResult_() == nil {
		return nil, errors.New("unexpected response from scheduler")
	}

	return resp.GetResult_().GetGetJobUpdateDetailsResult_().GetDetailsList(), nil
}

func (c *Client) RollbackJobUpdate(key aurora.JobUpdateKey, message string) error {

	c.logger.DebugPrintf("RollbackJobUpdate Thrift Payload: %+v %v\n", key, message)

	_, retryErr := c.thriftCallWithRetries(false, func() (*aurora.Response, error) {
		return c.client.RollbackJobUpdate(context.TODO(), &key, message)
	},
		nil,
	)

	if retryErr != nil {
		return errors.Wrap(retryErr, "unable to roll back job update")
	}
	return nil
}

func (c *Client) GetSchedulerURL() string {
	return c.config.url
}
