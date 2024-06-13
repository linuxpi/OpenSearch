/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.task.commons.clients.ListTaskRequest;
import org.opensearch.task.commons.clients.TaskManagerClient;
import org.opensearch.task.commons.task.Task;
import org.opensearch.task.commons.task.TaskType;
import org.opensearch.task.commons.worker.TaskWorker;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;

/**
 * Service to manage execution of tasks on Offline Node
 */
public class BackgroundTaskService implements LifecycleComponent {

    private final Logger logger = LogManager.getLogger(BackgroundTaskService.class);
    private final ThreadPool threadPool;
    private AsyncBackgroundTaskFetch backgroundTaskFetch;
    /**
     * TaskClient used to interact with TaskStore/Queue for Task Management and Coordination
     */
    private final TaskManagerClient taskClient;
    /**
     * Provides Corresponding TaskWorker to execute for each Task
     */
    private final Map<TaskType, TaskWorker> taskWorkerMap;

    public BackgroundTaskService(ThreadPool threadPool, TaskManagerClient taskClient, Map<TaskType, TaskWorker> taskWorkerMap) {
        this.threadPool = threadPool;
        this.taskClient = taskClient;
        this.taskWorkerMap = taskWorkerMap;
    }

    @Override
    public void start() {
        backgroundTaskFetch = new AsyncBackgroundTaskFetch(this);
    }

    @Override
    public void stop() {
        backgroundTaskFetch.close();
        backgroundTaskFetch.cancel();
    }

    @Override
    public void close() {
        stop();
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {

    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {

    }

    public void maybeExecuteTasks() {
        List<Task> assignedTasks = taskClient.getAssignedTasks(new ListTaskRequest());
        for (Task task : assignedTasks) {
            if (taskWorkerMap.containsKey(task.getTaskType())) {
                taskWorkerMap.get(task.getTaskType()).executeTask(task);
            }
        }
    }

    private class AsyncBackgroundTaskFetch extends AbstractAsyncTask {
        BackgroundTaskService backgroundTaskService;

        protected AsyncBackgroundTaskFetch(BackgroundTaskService backgroundTaskService) {
            super(backgroundTaskService.logger, backgroundTaskService.threadPool, new TimeValue(60000), true);
            this.backgroundTaskService = backgroundTaskService;
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        protected void runInternal() {
            backgroundTaskService.maybeExecuteTasks();
        }
    }
}
