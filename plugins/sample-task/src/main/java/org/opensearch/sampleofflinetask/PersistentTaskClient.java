/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sampleofflinetask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.indices.IndicesService;
import org.opensearch.offlinetasks.Task;
import org.opensearch.offlinetasks.TaskClient;
import org.opensearch.offlinetasks.TaskType;
import org.opensearch.persistent.PersistentTaskParams;
import org.opensearch.persistent.PersistentTasksCustomMetadata;
import org.opensearch.persistent.PersistentTasksService;
import org.opensearch.persistent.UpdatePersistentTaskStatusAction;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

/**
 * sample
 */
public class PersistentTaskClient implements TaskClient {

    private static final Logger logger = LogManager.getLogger(PersistentTaskClient.class);

    private PersistentTasksService persistentTasksService;
    private ClusterService clusterService;

    /**
     * sample
     * @param clusterService sample
     * @param persistentTasksService sample
     */
    public PersistentTaskClient(ClusterService clusterService, PersistentTasksService persistentTasksService) {
        this.clusterService = clusterService;
        this.persistentTasksService = persistentTasksService;
    }

    /**
     * sample
     * @param task sample
     */
    @Override
    public void submitTask(Task task) {
        // default implementation
        persistentTasksService.sendStartRequest("sample-task", "sample-task", new TaskParams(), ActionListener.wrap((a) -> {
            logger.info("successfully submitted persistent task {}", a);
        }, (a) -> {
            logger.error("failed to submit task", a);
        }));
//        clusterService.submitStateUpdateTask("submit new background task for offline fleet", new ClusterStateUpdateTask() {
//            @Override
//            public ClusterState execute(ClusterState currentState) throws Exception {
//                PersistentTasksCustomMetadata persistentTasks = currentState.metadata().custom(PersistentTasksCustomMetadata.TYPE);
//                PersistentTasksCustomMetadata updatedPersistentTasks = PersistentTasksCustomMetadata.builder(persistentTasks).addTask("sample-task", "sample-task", null, PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT).build();
//                return ClusterState.builder(currentState).metadata(Metadata.builder(currentState.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE, updatedPersistentTasks).build()).build();
//            }
//
//            @Override
//            public void onFailure(String source, Exception e) {
//                logger.error("failure to submit task", e);
//            }
//        });
    }

    /**
     * sample
     */
    @Override
    public List<Task> getUnassignedTasks() {
        PersistentTasksCustomMetadata persistentTasksCustomMetadata = clusterService.state().metadata().custom(PersistentTasksCustomMetadata.TYPE);
        Optional<PersistentTasksCustomMetadata.PersistentTask<?>> taskOptional = persistentTasksCustomMetadata.tasks().stream().filter(persistentTask -> persistentTask.isAssigned() == false).findFirst();

        if (taskOptional.isPresent()) {
            persistentTasksService.sendRemoveRequest(taskOptional.get().getId(), ActionListener.wrap(() -> {
                logger.info("successfully removed from tasks list");
            }));
            return List.of(new Task(taskOptional.get().getId(), TaskType.SAMPLE));
        } else {
            logger.error("no unassigned tasks found!");
            return List.of();
        }
    }

    /**
     * sample
     */
    public static class TaskParams implements PersistentTaskParams {

        /**
         * sample
         */
        public TaskParams() {

        }

        /**
         * sample
         * @param in sample
         */
        public TaskParams(StreamInput in) throws IOException {
            in.readVersion();
        }


        /**
         * sample
         */
        public static final String NAME = "sample-task";
        Version minVersion = Version.V_3_0_0;


        /**
         * sample
         */
        @Override
        public String getWriteableName() {
            return "sample-task";
        }

        /**
         * sample
         */
        @Override
        public Version getMinimalSupportedVersion() {
            return minVersion;
        }

        /**
         * sample
         * @param out sample
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVersion(minVersion);
        }

        /**
         * sample
         * @param builder
         * @param params
         * @return
         * @throws IOException
         */
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
//            builder.field("param", testParam);
            builder.endObject();
            return builder;
        }

    }

    /**
     * sample
     * @param parser
     * @return
     * @throws IOException
     */
    public static TaskParams fromXContent(XContentParser parser) throws IOException {
        return new TaskParams();
    }

}
