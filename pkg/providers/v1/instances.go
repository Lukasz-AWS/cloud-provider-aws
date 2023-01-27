/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aws

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// awsInstanceRegMatch represents Regex Match for AWS instance.
var awsInstanceRegMatch = regexp.MustCompile("^i-[^/]*$")

// InstanceID represents the ID of the instance in the AWS API, e.g. i-12345678
// The "traditional" format is "i-12345678"
// A new longer format is also being introduced: "i-12345678abcdef01"
// We should not assume anything about the length or format, though it seems
// reasonable to assume that instances will continue to start with "i-".
type InstanceID string

func (i InstanceID) awsString() *string {
	return aws.String(string(i))
}

// KubernetesInstanceID represents the id for an instance in the kubernetes API;
// the following form
//   - aws:///<zone>/<awsInstanceId>
//   - aws:////<awsInstanceId>
//   - aws:///<zone>/fargate-<eni-ip-address>
//   - <awsInstanceId>
type KubernetesInstanceID string

// EC2 describe instance caching and batching options
const (
	// max limit of k8s nodes support
	maxEC2ChannelSize = 8000
	// max number of in flight non batched ec2:DescribeInstances request to flow
	maxAllowedInflightRequest = 3
	// default wait interval for the ec2 instance id request which is already in flight
	defaultWaitInterval = 50 * time.Millisecond
	// Making sure the single instance calls waits max till 5 seconds 100* (50 * time.Millisecond)
	totalIterationForWaitInterval = 100
	// Maximum number of instances with which ec2:DescribeInstances call will be made
	maxInstancesBatchSize = 100
	// Maximum time in Milliseconds to wait for a new batch call this also depends on if the instance size has
	// already become 100 then it will not respect this limit
	maxWaitIntervalForBatch = 1000
)

type ec2Requests struct {
	set  map[string]bool
	lock sync.RWMutex
}

type ec2PrivateCache struct {
	cache map[string]*ec2.Instance
	lock  sync.RWMutex
}

// MapToAWSInstanceID extracts the InstanceID from the KubernetesInstanceID
func (name KubernetesInstanceID) MapToAWSInstanceID() (InstanceID, error) {
	s := string(name)

	if !strings.HasPrefix(s, "aws://") {
		// Assume a bare aws volume id (vol-1234...)
		// Build a URL with an empty host (AZ)
		s = "aws://" + "/" + "/" + s
	}
	url, err := url.Parse(s)
	if err != nil {
		return "", fmt.Errorf("Invalid instance name (%s): %v", name, err)
	}
	if url.Scheme != "aws" {
		return "", fmt.Errorf("Invalid scheme for AWS instance (%s)", name)
	}

	awsID := ""
	tokens := strings.Split(strings.Trim(url.Path, "/"), "/")
	// last token in the providerID is the aws resource ID for both EC2 and Fargate nodes
	if len(tokens) > 0 {
		awsID = tokens[len(tokens)-1]
	}

	// We sanity check the resulting volume; the two known formats are
	// i-12345678 and i-12345678abcdef01
	if awsID == "" || !(awsInstanceRegMatch.MatchString(awsID) || IsFargateNode(awsID)) {
		return "", fmt.Errorf("Invalid format for AWS instance (%s)", name)
	}

	return InstanceID(awsID), nil
}

// mapToAWSInstanceID extracts the InstanceIDs from the Nodes, returning an error if a Node cannot be mapped
func mapToAWSInstanceIDs(nodes []*v1.Node) ([]InstanceID, error) {
	var instanceIDs []InstanceID
	for _, node := range nodes {
		if node.Spec.ProviderID == "" {
			return nil, fmt.Errorf("node %q did not have ProviderID set", node.Name)
		}
		instanceID, err := KubernetesInstanceID(node.Spec.ProviderID).MapToAWSInstanceID()
		if err != nil {
			return nil, fmt.Errorf("unable to parse ProviderID %q for node %q", node.Spec.ProviderID, node.Name)
		}
		instanceIDs = append(instanceIDs, instanceID)
	}

	return instanceIDs, nil
}

// mapToAWSInstanceIDsTolerant extracts the InstanceIDs from the Nodes, skipping Nodes that cannot be mapped
func mapToAWSInstanceIDsTolerant(nodes []*v1.Node) []InstanceID {
	var instanceIDs []InstanceID
	for _, node := range nodes {
		if node.Spec.ProviderID == "" {
			klog.Warningf("node %q did not have ProviderID set", node.Name)
			continue
		}
		instanceID, err := KubernetesInstanceID(node.Spec.ProviderID).MapToAWSInstanceID()
		if err != nil {
			klog.Warningf("unable to parse ProviderID %q for node %q", node.Spec.ProviderID, node.Name)
			continue
		}
		instanceIDs = append(instanceIDs, instanceID)
	}

	return instanceIDs
}

// Gets the full information about this instance from the EC2 API
func describeInstance(ec2Client EC2, instanceID InstanceID) (*ec2.Instance, error) {
	request := &ec2.DescribeInstancesInput{
		InstanceIds: []*string{instanceID.awsString()},
	}

	instances, err := ec2Client.DescribeInstances(request)
	if err != nil {
		return nil, err
	}
	if len(instances) == 0 {
		return nil, fmt.Errorf("no instances found for instance: %s", instanceID)
	}
	if len(instances) > 1 {
		return nil, fmt.Errorf("multiple instances found for instance: %s", instanceID)
	}
	return instances[0], nil
}

// instanceCache manages the cache of DescribeInstances
type instanceCache struct {
	// TODO: Get rid of this field, send all calls through the instanceCache
	cloud *Cloud

	mutex    sync.Mutex
	snapshot *allInstancesSnapshot

	ec2Requests        ec2Requests
	ec2PrivateCache    ec2PrivateCache
	ec2RequestsChannel chan string
}

func (c *instanceCache) setec2PrivateCache(id string, ec2Instance *ec2.Instance) {
	c.ec2PrivateCache.lock.Lock()
	defer c.ec2PrivateCache.lock.Unlock()
	c.ec2PrivateCache.cache[id] = ec2Instance
}

func (c *instanceCache) getec2PrivateCache(id string) (*ec2.Instance, error) {
	c.ec2PrivateCache.lock.RLock()
	defer c.ec2PrivateCache.lock.RUnlock()
	if ec2Instance, ok := c.ec2PrivateCache.cache[id]; ok {
		return ec2Instance, nil
	}
	return nil, fmt.Errorf("instance id not found")
}

func (c *instanceCache) setec2RequestInFlight(id string) {
	c.ec2Requests.lock.Lock()
	defer c.ec2Requests.lock.Unlock()
	c.ec2Requests.set[id] = true
}

func (c *instanceCache) unsetec2RequestInFlight(id string) {
	c.ec2Requests.lock.Lock()
	defer c.ec2Requests.lock.Unlock()
	delete(c.ec2Requests.set, id)
}

func (c *instanceCache) getec2RequestInFlight(id string) bool {
	c.ec2Requests.lock.RLock()
	defer c.ec2Requests.lock.RUnlock()
	_, ok := c.ec2Requests.set[id]
	return ok
}

func (c *instanceCache) getec2RequestsInFlightSize() int {
	c.ec2Requests.lock.RLock()
	defer c.ec2Requests.lock.RUnlock()
	return len(c.ec2Requests.set)
}

// Calls ec2 API if not in cache
func (c *instanceCache) DescribeInstance(ec2Client EC2, id string) (*ec2.Instance, error) {
	instance, err := c.getec2PrivateCache(id)
	if err == nil {
		return instance, nil
	}
	klog.V(4).Infof("Missed the ec2PrivateCache for the InstanceId = %s Verifying if its already in ec2RequestsQueue ", id)
	// check if the request for instanceId already in queue.
	if c.getec2RequestInFlight(id) {
		klog.Infof("Found the InstanceId:= %s request In Queue waiting in 5 seconds loop ", id)
		for i := 0; i < totalIterationForWaitInterval; i++ {
			time.Sleep(defaultWaitInterval)
			instance, err := c.getec2PrivateCache(id)
			if err == nil {
				return instance, nil
			}
		}
		return nil, fmt.Errorf("failed to find node %s in ec2PrivateCache returning from loop", id)
	}

	klog.Infof("Missed the ec2RequestsQueue cache for the InstanceId = %s", id)
	c.setec2RequestInFlight(id)
	requestQueueLength := c.getec2RequestsInFlightSize()
	//The code verifies if the ec2Requests size is greater than max request in flight
	// then it writes to the channel where we are making batch ec2:DescribeInstances API call.
	if requestQueueLength > maxAllowedInflightRequest {
		klog.Infof("Writing to buffered channel for instance Id %s ", id)
		c.ec2RequestsChannel <- id
		return c.DescribeInstance(ec2Client, id)
	}

	klog.Infof("Calling ec2:DescribeInstances for the InstanceId = %s ", id)
	// Look up instance from EC2 API
	instance, err = describeInstance(ec2Client, InstanceID(id))
	if err != nil {
		c.unsetec2RequestInFlight(id)
		return nil, fmt.Errorf("failed querying EC2 API for node %s: %s ", id, err.Error())
	}
	c.setec2PrivateCache(id, instance)
	c.unsetec2RequestInFlight(id)

	return instance, nil
}

func (c *instanceCache) StartEc2DescribeBatchProcessing(ec2Client EC2) {
	startTime := time.Now()
	var instanceIdList []string
	for {
		var instanceId string
		select {
		case instanceId = <-c.ec2RequestsChannel:
			klog.V(4).Infof("Received the Instance Id := %s from buffered Channel for batch processing ", instanceId)
			instanceIdList = append(instanceIdList, instanceId)
		default:
			// Waiting for more elements to get added to the buffered Channel
			// And to support the for select loop.
			time.Sleep(20 * time.Millisecond)
		}
		endTime := time.Now()
		/*
		   The if statement checks for empty list and ignores to make any ec2:Describe API call
		   If elements are less than 100 and time of 200 millisecond has elapsed it will make the
		   ec2:DescribeInstances call with as many elements in the list.
		*/
		if (len(instanceIdList) > 0 && (endTime.Sub(startTime).Milliseconds()) > maxWaitIntervalForBatch) || len(instanceIdList) > maxInstancesBatchSize {
			startTime = time.Now()
			dupInstanceList := make([]string, len(instanceIdList))
			copy(dupInstanceList, instanceIdList)
			go c.getEC2InstancesAndPublishToCache(ec2Client, dupInstanceList)
			instanceIdList = nil
		}
	}
}
func (c *instanceCache) getEC2InstancesAndPublishToCache(ec2Client EC2, instanceIdList []string) {
	// Look up instance from EC2 API
	klog.Infof("Making Batch Query to DescribeInstances for %v instances ", len(instanceIdList))
	output, err := ec2Client.DescribeInstances(&ec2.DescribeInstancesInput{
		InstanceIds: aws.StringSlice(instanceIdList),
	})
	if err != nil {
		klog.Errorf("Batch call failed querying from EC2 API for nodes [%s] : with error = []%s ", instanceIdList, err.Error())
	} else {
		// Adding the result to ec2PrivateCache as well as removing from the requestQueueMap.
		for _, instance := range output {
			id := aws.StringValue(instance.InstanceId)
			c.setec2PrivateCache(id, instance)
		}
	}

	klog.V(4).Infof("Removing instances from request Queue after getting response from Ec2")
	for _, id := range instanceIdList {
		c.unsetec2RequestInFlight(id)
	}
}

// Gets the full information about these instance from the EC2 API. Caller must have acquired c.mutex before
// calling describeAllInstancesUncached.
func (c *instanceCache) describeAllInstancesUncached() (*allInstancesSnapshot, error) {
	now := time.Now()

	klog.V(4).Infof("EC2 DescribeInstances - fetching all instances")

	var filters []*ec2.Filter
	instances, err := c.cloud.describeInstances(filters)
	if err != nil {
		return nil, err
	}

	m := make(map[InstanceID]*ec2.Instance)
	for _, i := range instances {
		id := InstanceID(aws.StringValue(i.InstanceId))
		m[id] = i
	}

	snapshot := &allInstancesSnapshot{now, m}
	if c.snapshot != nil && snapshot.olderThan(c.snapshot) {
		// If this happens a lot, we could run this function in a mutex and only return one result
		klog.Infof("Not caching concurrent AWS DescribeInstances results")
	} else {
		c.snapshot = snapshot
	}

	return snapshot, nil
}

// cacheCriteria holds criteria that must hold to use a cached snapshot
type cacheCriteria struct {
	// MaxAge indicates the maximum age of a cached snapshot we can accept.
	// If set to 0 (i.e. unset), cached values will not time out because of age.
	MaxAge time.Duration

	// HasInstances is a list of InstanceIDs that must be in a cached snapshot for it to be considered valid.
	// If an instance is not found in the cached snapshot, the snapshot be ignored and we will re-fetch.
	HasInstances []InstanceID
}

// describeAllInstancesCached returns all instances, using cached results if applicable
func (c *instanceCache) describeAllInstancesCached(criteria cacheCriteria) (*allInstancesSnapshot, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.snapshot != nil && c.snapshot.MeetsCriteria(criteria) {
		klog.V(6).Infof("EC2 DescribeInstances - using cached results")
		return c.snapshot, nil
	}

	return c.describeAllInstancesUncached()
}

// olderThan is a simple helper to encapsulate timestamp comparison
func (s *allInstancesSnapshot) olderThan(other *allInstancesSnapshot) bool {
	// After() is technically broken by time changes until we have monotonic time
	return other.timestamp.After(s.timestamp)
}

// MeetsCriteria returns true if the snapshot meets the criteria in cacheCriteria
func (s *allInstancesSnapshot) MeetsCriteria(criteria cacheCriteria) bool {
	if criteria.MaxAge > 0 {
		// Sub() is technically broken by time changes until we have monotonic time
		now := time.Now()
		if now.Sub(s.timestamp) > criteria.MaxAge {
			klog.V(6).Infof("instanceCache snapshot cannot be used as is older than MaxAge=%s", criteria.MaxAge)
			return false
		}
	}

	if len(criteria.HasInstances) != 0 {
		for _, id := range criteria.HasInstances {
			if nil == s.instances[id] {
				klog.V(6).Infof("instanceCache snapshot cannot be used as does not contain instance %s", id)
				return false
			}
		}
	}

	return true
}

// allInstancesSnapshot holds the results from querying for all instances,
// along with the timestamp for cache-invalidation purposes
type allInstancesSnapshot struct {
	timestamp time.Time
	instances map[InstanceID]*ec2.Instance
}

// FindInstances returns the instances corresponding to the specified ids.  If an id is not found, it is ignored.
func (s *allInstancesSnapshot) FindInstances(ids []InstanceID) map[InstanceID]*ec2.Instance {
	m := make(map[InstanceID]*ec2.Instance)
	for _, id := range ids {
		instance := s.instances[id]
		if instance != nil {
			m[id] = instance
		}
	}
	return m
}
