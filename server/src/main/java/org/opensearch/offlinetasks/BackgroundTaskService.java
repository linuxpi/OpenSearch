/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offlinetasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * sample
 */
public class BackgroundTaskService {
    private static final Logger logger = LogManager.getLogger(IndicesService.class);

    public static final Setting<TimeValue> SWEEP_PERIOD = Setting.positiveTimeSetting(
        "background.taskservice.sweeper.period",
        TimeValue.timeValueSeconds(10),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );

    private ExecutorService executorService;
    private ThreadPool threadPool;
    private TaskClient taskClient;

    private Map<TaskType, TaskWorker> taskWorkers = new HashMap();

    public BackgroundTaskService(ThreadPool threadPool) {
        logger.info("Background service init...");
        this.threadPool = threadPool;
        this.executorService = Executors.newSingleThreadExecutor(OpenSearchExecutors.daemonThreadFactory("background-tasks-service"));
    }

    public void registerTask(TaskType taskType, TaskWorker worker) {
        logger.info(String.format(Locale.ROOT, "Registering TaskWorker %s %s", taskType, worker));
        taskWorkers.put(taskType, worker);
    }

    public void registerTaskClient(TaskClient taskClient) {
        logger.info(String.format(Locale.ROOT, "Registering TaskClient %s", taskClient.getClass().getSimpleName()));
        this.taskClient = taskClient;
    }

    public void runTask(Task task) {
        logger.info(String.format(Locale.ROOT, "Executing task %s", task.toString()));
        TaskWorker worker = taskWorkers.getOrDefault(task.getType(), null);
        if (worker == null) {
            logger.info(String.format(Locale.ROOT, "Skipping as no task worker registered for task - %s", task.getType()));
            return;
        }
        worker.doWork(task);
    }

    public void execute(TaskClient taskClient) {
        try {
            logger.info("Executing tasks...");
            taskClient.submitTask(new Task("sample-task", TaskType.SAMPLE));
            taskClient.submitTask(new Task("sample-task", TaskType.MERGE));
            List<Task> tasks = taskClient.getUnassignedTasks();
            logger.info(String.format(Locale.ROOT, "task list -> %s", tasks));
            tasks.forEach(this::runTask);
        } catch (Exception ex) {
            logger.error("failed to execute tasks", ex);
        }
    }

    public void start(TaskClient taskClient) {
        logger.info("Scheduled background sweeper");
        this.threadPool.scheduleWithFixedDelay(() -> executorService.submit(() -> this.execute(taskClient)), TimeValue.timeValueSeconds(10), ThreadPool.Names.REMOTE_PURGE);
    }

}
