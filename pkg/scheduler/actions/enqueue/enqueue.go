/*
Copyright 2019 The Volcano Authors.

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

package enqueue

import (
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (enqueue *Action) Name() string {
	return "enqueue"
}

func (enqueue *Action) Initialize() {}

func (enqueue *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Enqueue ...")
	defer klog.V(5).Infof("Leaving Enqueue ...")

	queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	queueSet := sets.NewString()
	jobsMap := map[api.QueueID]*util.PriorityQueue{}

	for _, job := range ssn.Jobs {
		if job.ScheduleStartTimestamp.IsZero() {
			ssn.Jobs[job.UID].ScheduleStartTimestamp = metav1.Time{
				Time: time.Now(),
			}
		}
		if queue, found := ssn.Queues[job.Queue]; !found {
			klog.Errorf("Failed to find Queue <%s> for Job <%s/%s>",
				job.Queue, job.Namespace, job.Name)
			continue
		} else if !queueSet.Has(string(queue.UID)) {
			klog.V(5).Infof("Added Queue <%s> for Job <%s/%s>",
				queue.Name, job.Namespace, job.Name)

			queueSet.Insert(string(queue.UID))
			queues.Push(queue)
		}

		if job.IsPending() {
			if _, found := jobsMap[job.Queue]; !found {
				jobsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
			}
			klog.V(5).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
			jobsMap[job.Queue].Push(job)
		}
	}

	klog.V(3).Infof("Try to enqueue PodGroup to %d Queues", len(jobsMap))

	// Phase 1: Normal enqueue loop (existing logic unchanged)
	for {
		if queues.Empty() {
			break
		}

		queue := queues.Pop().(*api.QueueInfo)

		// skip the Queue that has no pending job
		jobs, found := jobsMap[queue.UID]
		if !found || jobs.Empty() {
			continue
		}
		job := jobs.Pop().(*api.JobInfo)

		if job.PodGroup.Spec.MinResources == nil || ssn.JobEnqueueable(job) {
			ssn.JobEnqueued(job)
			job.PodGroup.Status.Phase = scheduling.PodGroupInqueue
			job.EnqueueTimestamp = metav1.Time{Time: time.Now()}
			ssn.Jobs[job.UID] = job
		}

		// Added Queue back until no job in Queue.
		queues.Push(queue)
	}

	// Phase 2: Inqueue Preemption
	tryPreemptForPendingJobs(ssn)
}

func (enqueue *Action) UnInitialize() {}

// scoredCandidate holds a candidate job with its priority and enqueue time for sorting.
type scoredCandidate struct {
	job         *api.JobInfo
	priority    int32
	enqueueTime time.Time
}

// tryPreemptForPendingJobs processes remaining Pending jobs that failed Phase 1,
// attempting to preempt lower-priority Inqueue jobs in the same queue.
// Jobs are processed in queue-priority then job-priority order.
func tryPreemptForPendingJobs(ssn *framework.Session) {
	// Collect remaining Pending jobs grouped by queue
	pendingByQueue := map[api.QueueID][]*api.JobInfo{}
	for _, job := range ssn.Jobs {
		if job.IsPending() && job.PodGroup.Spec.MinResources != nil {
			pendingByQueue[job.Queue] = append(pendingByQueue[job.Queue], job)
		}
	}

	if len(pendingByQueue) == 0 {
		return
	}

	// Build and sort queues list by QueueOrderFn
	var queuesWithPending []*api.QueueInfo
	for queueID := range pendingByQueue {
		if queue, found := ssn.Queues[queueID]; found {
			queuesWithPending = append(queuesWithPending, queue)
		}
	}
	sort.Slice(queuesWithPending, func(i, j int) bool {
		return ssn.QueueOrderFn(queuesWithPending[i], queuesWithPending[j])
	})

	for _, queue := range queuesWithPending {
		pendingJobs := pendingByQueue[queue.UID]
		// Sort pending jobs by JobOrderFn (higher priority first)
		sort.Slice(pendingJobs, func(i, j int) bool {
			return ssn.JobOrderFn(pendingJobs[i], pendingJobs[j])
		})

		for _, preemptor := range pendingJobs {
			// Skip if already enqueued (may have been enqueued during Phase 2 processing)
			if !preemptor.IsPending() {
				continue
			}

			// Find Inqueue candidates in the same queue with strictly lower priority
			var candidates []*api.JobInfo
			for _, job := range ssn.Jobs {
				if job.Queue == preemptor.Queue &&
					job.PodGroup.Status.Phase == scheduling.PodGroupInqueue &&
					job.Priority < preemptor.Priority {
					candidates = append(candidates, job)
				}
			}

			if len(candidates) == 0 {
				continue
			}

			victims := selectVictims(preemptor, candidates)
			if len(victims) == 0 {
				continue
			}

			klog.V(3).Infof("Inqueue preemption: preemptor <%s/%s> (pri=%d) preempts %d victims",
				preemptor.Namespace, preemptor.Name, preemptor.Priority, len(victims))

			// Execute preemption
			for _, victim := range victims {
				ssn.JobPreempted(victim)
				victim.PodGroup.Status.Phase = scheduling.PodGroupPending
				ssn.Jobs[victim.UID] = victim
				klog.V(4).Infof("Preempted victim <%s/%s> (pri=%d) from inqueue back to pending",
					victim.Namespace, victim.Name, victim.Priority)
			}

			// Enqueue preemptor
			ssn.JobEnqueued(preemptor)
			preemptor.PodGroup.Status.Phase = scheduling.PodGroupInqueue
			preemptor.EnqueueTimestamp = metav1.Time{Time: time.Now()}
			ssn.Jobs[preemptor.UID] = preemptor
			klog.V(3).Infof("Inqueue preemption: preemptor <%s/%s> enqueued successfully",
				preemptor.Namespace, preemptor.Name)
		}
	}
}

// selectVictims selects Inqueue jobs to preempt using a greedy strategy with two-level sorting:
// 1. Primary sort: ascending by job priority (lower priority preempted first)
// 2. Secondary sort: ascending by enqueue time (longer enqueued preempted first)
// Returns victims whose total MinResources >= preemptor.MinResources, or nil if not enough.
func selectVictims(preemptor *api.JobInfo, candidates []*api.JobInfo) []*api.JobInfo {
	scored := make([]scoredCandidate, 0, len(candidates))
	for _, c := range candidates {
		scored = append(scored, scoredCandidate{
			job:         c,
			priority:    c.Priority,
			enqueueTime: c.EnqueueTimestamp.Time,
		})
	}

	// Sort: lower priority first; within same priority, longer enqueue time first
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].priority != scored[j].priority {
			return scored[i].priority < scored[j].priority
		}
		return scored[i].enqueueTime.Before(scored[j].enqueueTime)
	})

	preemptorMinRes := preemptor.GetMinResources()
	var victims []*api.JobInfo
	accumulated := api.EmptyResource()

	for _, sc := range scored {
		accumulated.Add(sc.job.GetMinResources())
		victims = append(victims, sc.job)
		if preemptorMinRes.LessEqual(accumulated, api.Zero) {
			return victims
		}
	}

	// Not enough resources from all candidates
	return nil
}
