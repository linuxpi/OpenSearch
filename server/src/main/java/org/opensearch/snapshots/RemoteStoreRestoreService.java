/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.metadata.MetadataIndexUpgradeService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.*;

public class RemoteStoreRestoreService implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(RemoteStoreRestoreService.class);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final MetadataCreateIndexService createIndexService;

    private final MetadataIndexUpgradeService metadataIndexUpgradeService;

    private final ShardLimitValidator shardLimitValidator;

    private final ClusterSettings clusterSettings;

    public RemoteStoreRestoreService(
        ClusterService clusterService,
        AllocationService allocationService,
        MetadataCreateIndexService createIndexService,
        MetadataIndexUpgradeService metadataIndexUpgradeService,
        ClusterSettings clusterSettings,
        ShardLimitValidator shardLimitValidator
    ) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.createIndexService = createIndexService;
        this.metadataIndexUpgradeService = metadataIndexUpgradeService;
        if (DiscoveryNode.isClusterManagerNode(clusterService.getSettings())) {
            clusterService.addStateApplier(this);
        }
        this.clusterSettings = clusterService.getClusterSettings();
        this.shardLimitValidator = shardLimitValidator;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {

    }

    /**
     * Restores data from remote store for indices specified in the restore request.
     *
     * @param request  restore request
     * @param listener restore listener
     */
    public void restore(RestoreRemoteStoreRequest request, final ActionListener<RestoreCompletionResponse> listener) {
        clusterService.submitStateUpdateTask("restore[remote_store]", new ClusterStateUpdateTask() {
            final String restoreUUID = UUIDs.randomBase64UUID();
            RestoreInfo restoreInfo = null;

            private IndexMetadata getRemoteIndexMetadata() {
                // Dummy data for initial testing
                try {
                    return IndexMetadata.builder("my-index-01")
                        .settings(Settings.builder()
                            .put(SETTING_INDEX_UUID, "TLHafcwfTAazM5hFSFidyA")
                            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                            .put(SETTING_REMOTE_STORE_ENABLED, true)
                            .put(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "my-fs-repository")
                            .put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "my-fs-repository")
                            .put(SETTING_NUMBER_OF_SHARDS, 1)
                            .put(SETTING_NUMBER_OF_REPLICAS, 0)
                            .put(SETTING_VERSION_CREATED, "137217827")
                        )
                        .primaryTerm(0, 2)
                        .putMapping("{\"_doc\":{\"properties\":{\"settings\":{\"properties\":{\"index\":{\"properties\":{\"number_of_replicas\":{\"type\":\"long\"},\"number_of_shards\":{\"type\":\"long\"},\"remote_store\":{\"properties\":{\"enabled\":{\"type\":\"boolean\"},\"repository\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"translog\":{\"properties\":{\"buffer_interval\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"enabled\":{\"type\":\"boolean\"},\"repository\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}}},\"replication\":{\"properties\":{\"type\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}}}}}}}}")
                        .build();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            private boolean isIndexMetadataFromRemoteStore(IndexMetadata indexMetadata) {
                // might move it somewhere else.
                // but we need a way to distinguish if we are restore IndexMetadata from restore.
                return true;
            }

            private void validate(ClusterState currentState, Map<String, IndexMetadata> indexMetadataMap,
                                  boolean allowPartial, boolean restoreAllShards) {
                for (Map.Entry<String, IndexMetadata> indexMetadataSet: indexMetadataMap.entrySet()) {
                    String indexName = indexMetadataSet.getKey();
                    IndexMetadata indexMetadata = indexMetadataSet.getValue();
                    if (indexMetadata.getSettings().getAsBoolean(SETTING_REMOTE_STORE_ENABLED, false)) {
                        if (restoreAllShards && IndexMetadata.State.CLOSE.equals(indexMetadata.getState())) {
                            String errorMsg = "cannot restore index ["
                                + indexName
                                + "] because an open index "
                                + "with same name already exists in the cluster. Close the existing index";
                            if (allowPartial) {
                                throw new IllegalStateException(errorMsg);
                            } else {
                                logger.warn(errorMsg);
                            }
                        }

                        if (isIndexMetadataFromRemoteStore(indexMetadata)) {
                            Version minIndexCompatibilityVersion = currentState.getNodes()
                                .getMaxNodeVersion()
                                .minimumIndexCompatibilityVersion();
                            metadataIndexUpgradeService.upgradeIndexMetadata(indexMetadata, minIndexCompatibilityVersion);
                            boolean isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(indexMetadata.getSettings());
                            createIndexService.validateIndexName(indexName, currentState);
                            createIndexService.validateDotIndex(indexName, isHidden);
                            createIndexService.validateIndexSettings(indexName, indexMetadata.getSettings(), false);
                        }
                        // TODO other validation will come here. still figuring out what else we need to validate
                    } else {
                        logger.warn("Remote store is not enabled for index: {}", indexName);
                    }

                }
            }

            private ClusterState executeRestore(ClusterState currentState, Map<String, IndexMetadata> indexMetadataMap,
                                                boolean restoreAllShards) {
                List<String> indicesToBeRestored = new ArrayList<>();
                int totalShards = 0;
                ClusterState.Builder builder = ClusterState.builder(currentState);
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
                for (Map.Entry<String, IndexMetadata> indexMetadataSet : indexMetadataMap.entrySet()) {
                    String indexName = indexMetadataSet.getKey();
                    IndexMetadata indexMetadata = indexMetadataSet.getValue();
                    IndexMetadata updatedIndexMetadata = indexMetadata;
                    Map<ShardId, ShardRouting> activeInitializingShards = new HashMap<>();
                    if (restoreAllShards) {
                        updatedIndexMetadata = IndexMetadata.builder(indexMetadata)
                            .state(IndexMetadata.State.OPEN)
                            // do we need to increment this during restore from remote index metadata
                            .version(1 + indexMetadata.getVersion())
                            .mappingVersion(1 + indexMetadata.getMappingVersion())
                            .settingsVersion(1 + indexMetadata.getSettingsVersion())
                            .aliasesVersion(1 + indexMetadata.getAliasesVersion())
                            .build();
                    } else if (isIndexMetadataFromRemoteStore(indexMetadata) == false) {
                        activeInitializingShards = currentState.routingTable()
                            .index(indexName)
                            .shards()
                            .values()
                            .stream()
                            .map(IndexShardRoutingTable::primaryShard)
                            .filter(shardRouting -> shardRouting.unassigned() == false)
                            .collect(Collectors.toMap(ShardRouting::shardId, Function.identity()));
                    }

                    IndexId indexId = new IndexId(indexName, updatedIndexMetadata.getIndexUUID());

                    RecoverySource.RemoteStoreRecoverySource recoverySource = new RecoverySource.RemoteStoreRecoverySource(
                        restoreUUID,
                        updatedIndexMetadata.getCreationVersion(),
                        indexId
                    );
                    rtBuilder.addAsRemoteStoreRestore(updatedIndexMetadata, recoverySource, activeInitializingShards);
                    blocks.updateBlocks(updatedIndexMetadata);
                    mdBuilder.put(updatedIndexMetadata, true);
                    indicesToBeRestored.add(indexName);
                    totalShards += updatedIndexMetadata.getNumberOfShards();
                }

                restoreInfo = new RestoreInfo("remote_store", indicesToBeRestored, totalShards, totalShards);

                RoutingTable rt = rtBuilder.build();
                ClusterState updatedState = builder.metadata(mdBuilder).blocks(blocks).routingTable(rt).build();
                return allocationService.reroute(updatedState, "restored from remote store");
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
                indexMetadataMap.put("my-index-01", getRemoteIndexMetadata());
                validate(currentState, indexMetadataMap, true, request.restoreAllShards());
                return executeRestore(currentState, indexMetadataMap, request.restoreAllShards());
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to restore from remote store", e);
                listener.onFailure(e);
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new RestoreCompletionResponse(restoreUUID, restoreInfo));
            }
        });

    }

    // We can think of making this generic and use one in both RestoreService and RemoteStoreRestoreService
    public static final class RestoreCompletionResponse {
        private final String uuid;
        private final RestoreInfo restoreInfo;

        private RestoreCompletionResponse(final String uuid, final RestoreInfo restoreInfo) {
            this.uuid = uuid;
            this.restoreInfo = restoreInfo;
        }

        public String getUuid() {
            return uuid;
        }

        public RestoreInfo getRestoreInfo() {
            return restoreInfo;
        }
    }

}
