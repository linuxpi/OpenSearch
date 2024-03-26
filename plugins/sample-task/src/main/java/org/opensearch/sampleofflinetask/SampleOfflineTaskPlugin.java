/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sampleofflinetask;

import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.offlinetasks.*;
import org.opensearch.persistent.*;
import org.opensearch.plugins.PersistentTaskPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;


/**
 * sample
 */
public class SampleOfflineTaskPlugin extends Plugin implements TaskWorkerPlugin, TaskClientPlugin, PersistentTaskPlugin {

    Settings settings;

    /**
     * sample
     * @param settings sample
     */
    public SampleOfflineTaskPlugin(Settings settings) {
        this.settings = settings;
    }

    /**
     * sample
     * @param client A client to make requests to the system
     * @param clusterService A service to allow watching and updating cluster state
     * @param threadPool A service to allow retrieving an executor to run an async action
     * @param resourceWatcherService A service to watch for changes to node local files
     * @param scriptService A service to allow running scripts on the local node
     * @param xContentRegistry the registry for extensible xContent parsing
     * @param environment the environment for path and setting configurations
     * @param nodeEnvironment the node environment used coordinate access to the data paths
     * @param namedWriteableRegistry the registry for {@link org.opensearch.core.common.io.stream.NamedWriteable} object parsing
     * @param indexNameExpressionResolver A service that resolves expression to index and alias names
     * @param repositoriesServiceSupplier A supplier for the service that manages snapshot repositories; will return null when this method
     *                                   is called, but will return the repositories service once the node is initialized.
     * @return sample
     */
    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        TaskClient taskClient = new PersistentTaskClient(clusterService, new PersistentTasksService(clusterService, threadPool, client));
        return Collections.singletonList(taskClient);
    }

    /**
     * sample
     */
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(new NamedWriteableRegistry.Entry(
            PersistentTaskParams.class,
            PersistentTaskClient.TaskParams.NAME,
            PersistentTaskClient.TaskParams::new
        ));
    }

    /**
     * sample
     */
    public TaskType getType() { return TaskType.SAMPLE; }

    /**
     * sample
     */
    @Override
    public TaskWorker registerTaskWorker() {
        return new SampleTaskWorker();
    }

    /**
     * sample
     */
    @Override
    public TaskClient registerTaskClient() {
        return new SampleTaskClient();
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return List.of(new PersistentTasksExecutor<PersistentTaskClient.TaskParams>("sample-task", "sample-task") {
            @Override
            protected void nodeOperation(AllocatedPersistentTask task, PersistentTaskClient.TaskParams params, PersistentTaskState state) {
                // no op worker
            }
        });
    }
}
