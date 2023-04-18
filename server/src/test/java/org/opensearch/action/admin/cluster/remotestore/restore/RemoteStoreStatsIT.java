/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.restore;

import org.junit.After;
import org.junit.Before;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_STORE_ENABLED_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_STORE_REPOSITORY_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_TRANSLOG_REPOSITORY_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_TRANSLOG_STORE_ENABLED_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class RemoteStoreStatsIT extends OpenSearchIntegTestCase {

    private static final String REPOSITORY_NAME = "test-remore-store-repo";
    private static final String INDEX_NAME = "remote-store-test-idx-1";
    private static final String TOTAL_OPERATIONS = "total-operations";
    private static final String REFRESHED_OR_FLUSHED_OPERATIONS = "refreshed-or-flushed-operations";
    private static final String MAX_SEQ_NO_TOTAL = "max-seq-no-total";
    private static final String MAX_SEQ_NO_REFRESHED_OR_FLUSHED = "max-seq-no-refreshed-or-flushed";

    @Override
    protected Settings nodeSettings(int nodeOriginal) {
        Settings settings = super.nodeSettings(nodeOriginal);
        Settings.Builder builder = Settings.builder()
            .put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(CLUSTER_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
            .put(CLUSTER_REMOTE_STORE_REPOSITORY_SETTING.getKey(), "my-segment-repo-1")
            .put(CLUSTER_REMOTE_TRANSLOG_STORE_ENABLED_SETTING.getKey(), true)
            .put(CLUSTER_REMOTE_TRANSLOG_REPOSITORY_SETTING.getKey(), "my-translog-repo-1")
            .put(settings);
        return builder.build();
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL, "true")
            .put(FeatureFlags.REPLICATION_TYPE, "true")
            .put(FeatureFlags.REMOTE_STORE, "true")
            .build();
    }

    @Before
    public void setup() {
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        FeatureFlagSetter.set(FeatureFlags.REPLICATION_TYPE);
        internalCluster().startClusterManagerOnlyNode();
        Path absolutePath = randomRepoPath().toAbsolutePath();
        assertAcked(
            clusterAdmin().preparePutRepository("my-segment-repo-1")
                .setType("fs")
                .setSettings(Settings.builder().put("location", absolutePath))
        );
        assertAcked(
            clusterAdmin().preparePutRepository("my-translog-repo-1")
                .setType("fs")
                .setSettings(Settings.builder().put("location", absolutePath))
        );
        assertAcked(
            clusterAdmin().preparePutRepository("my-custom-repo")
                .setType("fs")
                .setSettings(Settings.builder().put("location", absolutePath))
        );
    }

    public void testDefaultRemoteStoreNoUserOverride() throws Exception {
        final int numReplicas = internalCluster().numDataNodes();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices("test-idx-1").includeDefaults(true))
            .get();
        Thread.sleep(3000);
//        Settings indexSettings = getIndexResponse.settings().get("test-idx-1");
        RemoteStoreStatsResponse remoteStoreStatsResponse = client().admin().cluster().prepareRemoteStoreStats().execute().actionGet();
        remoteStoreStatsResponse.getShards();
    }

}
