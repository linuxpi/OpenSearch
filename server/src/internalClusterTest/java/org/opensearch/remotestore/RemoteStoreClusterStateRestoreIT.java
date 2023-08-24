/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.opensearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.opensearch.indices.ShardLimitValidator.SETTING_MAX_SHARDS_PER_CLUSTER_KEY;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class RemoteStoreClusterStateRestoreIT extends BaseRemoteStoreRestoreIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME))
            // TODO uncomment after rebased with upload changes
            // .put(CLUSTER_REMOTE_STATE_REPOSITORY_SETTING.getKey(), REPOSITORY_NAME)
            .build();
    }

    private void stopAllNodes() {
        try {
            int totalDataNodes = internalCluster().numDataNodes();
            while (totalDataNodes > 0) {
                internalCluster().stopRandomDataNode();
                totalDataNodes -= 1;
            }
            int totalClusterManagerNodes = internalCluster().numClusterManagerNodes();
            while (totalClusterManagerNodes > 1) {
                internalCluster().stopRandomNonClusterManagerNode();
                totalClusterManagerNodes -= 1;
            }
            internalCluster().stopCurrentClusterManagerNode();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void addNewNodes(int dataNodeCount, int clusterManagerNodeCount) {
        internalCluster().startNodes(dataNodeCount + clusterManagerNodeCount);
    }

    private Map<String, Long> initialTestSetup(int shardCount, int replicaCount, int dataNodeCount, int clusterManagerNodeCount) {
        prepareCluster(clusterManagerNodeCount, dataNodeCount, INDEX_NAME, replicaCount, shardCount);
        Map<String, Long> indexStats = indexData(1, false, INDEX_NAME);
        assertEquals(shardCount, getNumShards(INDEX_NAME).totalNumShards);
        ensureGreen(INDEX_NAME);
        return indexStats;
    }

    private void resetCluster(int dataNodeCount, int clusterManagerNodeCount) {
        stopAllNodes();
        addNewNodes(dataNodeCount, clusterManagerNodeCount);
        putRepository(absolutePath);
        putRepository(absolutePath2, REPOSITORY_2_NAME);
    }

    private void restoreAndValidate(String clusterUUID, Map<String, Long> indexStats) {
        restoreAndValidate(clusterUUID, indexStats, true, PlainActionFuture.newFuture());
    }

    private void restoreAndValidate(
        String clusterUUID,
        Map<String, Long> indexStats,
        boolean validate,
        ActionListener<RestoreRemoteStoreResponse> actionListener
    ) {
        client().admin()
            .cluster()
            // Any sampleUUID would work as we are not integrated with remote cluster state repo in this test.
            // We are mocking that interaction and supplying dummy index metadata
            .restoreRemoteStore(new RestoreRemoteStoreRequest().clusterUUID(clusterUUID), actionListener);

        if (validate) {
            // Step - 4 validation restore is successful.
            ensureGreen(INDEX_NAME);
            verifyRestoredData(indexStats, true, INDEX_NAME);
        }
    }

    private void restoreAndValidateFails(String clusterUUID, PlainActionFuture<RestoreRemoteStoreResponse> actionListener) {
        restoreAndValidate(clusterUUID, null, false, actionListener);

        try {
            RestoreRemoteStoreResponse response = actionListener.get();
        } catch (ExecutionException e) {
            // If the request goes to co-ordinator, e.getCause() can be RemoteTransportException
            assertTrue(e.getCause() instanceof IllegalStateException || e.getCause().getCause() instanceof IllegalStateException);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void testFullClusterRestore() throws IOException {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 0;
        int dataNodeCount = shardCount;
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        Map<String, Long> indexStats = initialTestSetup(shardCount, replicaCount, dataNodeCount, 1);
        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        resetCluster(dataNodeCount, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 3 Trigger full cluster restore and validate
        restoreAndValidate(prevClusterUUID, indexStats);
    }

    @AwaitsFix(bugUrl = "waiting upload flow rebase. tested on integration PR")
    public void testFullClusterRestoreMultipleIndices() {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 0;
        int dataNodeCount = shardCount + 1;
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        Map<String, Long> indexStats = initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);

        String secondIndexName = INDEX_NAME + "-2";
        createIndex(secondIndexName, remoteStoreIndexSettings(replicaCount, shardCount + 1));
        Map<String, Long> indexStats2 = indexData(1, false, secondIndexName);
        assertEquals(shardCount + 1, getNumShards(secondIndexName).totalNumShards);
        ensureGreen(secondIndexName);

        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        resetCluster(dataNodeCount, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 3 Trigger full cluster restore
        restoreAndValidate(prevClusterUUID, indexStats);
        ensureGreen(secondIndexName);
        verifyRestoredData(indexStats2, true, secondIndexName);
    }

    @AwaitsFix(bugUrl = "waiting upload flow rebase. tested on integration PR")
    public void testFullClusterRestoreShardLimitReached() {
        int shardCount = randomIntBetween(2, 3);
        int replicaCount = 0;
        int dataNodeCount = shardCount;
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        Map<String, Long> indexStats = initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);

        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        // Restarting cluster with just 1 data node helps with applying cluster settings
        resetCluster(1, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step 3 - Reduce shard limits to hit shard limit with less no of shards
        try {
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest().transientSettings(
                        Settings.builder().put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 1).put(SETTING_MAX_SHARDS_PER_CLUSTER_KEY, 1)
                    )
                )
                .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        // Step - 4 Trigger full cluster restore and validate it fails
        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        restoreAndValidate(prevClusterUUID, indexStats, false, future);

        try {
            RestoreRemoteStoreResponse response = future.get();
        } catch (ExecutionException e) {
            // If the request goes to co-ordinator, e.getCause() can be RemoteTransportException
            assertTrue(e.getCause() instanceof IllegalArgumentException || e.getCause().getCause() instanceof IllegalArgumentException);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Step - 5 Reset the cluster settings
        ClusterUpdateSettingsRequest resetRequest = new ClusterUpdateSettingsRequest();
        resetRequest.transientSettings(
            Settings.builder().putNull(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey()).putNull(SETTING_MAX_SHARDS_PER_CLUSTER_KEY)
        );

        try {
            client().admin().cluster().updateSettings(resetRequest).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @AwaitsFix(bugUrl = "waiting upload flow rebase. tested on integration PR")
    public void testFullClusterRestoreNoStateInRestoreIllegalStateArgumentException() {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 0;
        int dataNodeCount = shardCount;
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);

        // Step - 2 Trigger full cluster restore and validate it fails
        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        restoreAndValidateFails("randomUUID", future);
    }

    @AwaitsFix(bugUrl = "waiting upload flow rebase. tested on integration PR")
    public void testRestoreFlowFullClusterOnSameClusterUUID() {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 0;
        int dataNodeCount = shardCount;
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);

        // Step - 2 Trigger full cluster restore and validate it fails
        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        restoreAndValidateFails(clusterService().state().metadata().clusterUUID(), future);
    }

    @AwaitsFix(bugUrl = "waiting upload flow rebase. tested on integration PR")
    public void testFullClusterRestoreSameNameIndexExists() {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 0;
        int dataNodeCount = shardCount;
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);

        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        resetCluster(dataNodeCount, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 3 Create a new index with same name
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 1));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        // Step - 4 Trigger full cluster restore and validate fails
        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        restoreAndValidateFails(prevClusterUUID, future);

        // Step - 4 validation restore is successful.
        ensureGreen(INDEX_NAME);
    }

    @AwaitsFix(bugUrl = "waiting upload flow rebase. tested on integration PR")
    public void testFullClusterRestoreMarkerFilePointsToInvalidIndexMetadataPathIllegalStateArgumentException() {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 0;
        int dataNodeCount = shardCount;
        int clusterManagerNodeCount = 1;

        // Step - 1 index some data to generate files in remote directory
        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);

        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Step - 2 Replace all nodes in the cluster with new nodes. This ensures new cluster state doesn't have previous index metadata
        resetCluster(dataNodeCount, clusterManagerNodeCount);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 4 Delete index metadata file in remote
        try {
            Files.move(
                absolutePath.resolve(clusterService().state().getClusterName().value() + "/cluster-state/" + prevClusterUUID + "/index"),
                absolutePath.resolve("cluster-state/")
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Step - 5 Trigger full cluster restore and validate fails
        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        restoreAndValidateFails(prevClusterUUID, future);
    }
}
