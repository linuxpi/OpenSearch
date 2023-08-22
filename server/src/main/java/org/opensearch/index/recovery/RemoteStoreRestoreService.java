/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.metadata.MetadataIndexUpgradeService;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.store.RemoteClusterStateService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.RestoreInfo;
import org.opensearch.snapshots.RestoreService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;

/**
 * Service responsible for restoring index data from remote store
 *
 * @opensearch.internal
 */
public class RemoteStoreRestoreService {
    private static final Logger logger = LogManager.getLogger(RemoteStoreRestoreService.class);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final MetadataCreateIndexService createIndexService;

    private final MetadataIndexUpgradeService metadataIndexUpgradeService;

    private final ShardLimitValidator shardLimitValidator;

    private final RemoteClusterStateService remoteClusterStateService;

    public RemoteStoreRestoreService(
        ClusterService clusterService,
        AllocationService allocationService,
        MetadataCreateIndexService createIndexService,
        MetadataIndexUpgradeService metadataIndexUpgradeService,
        ShardLimitValidator shardLimitValidator,
        RemoteClusterStateService remoteClusterStateService
    ) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.createIndexService = createIndexService;
        this.metadataIndexUpgradeService = metadataIndexUpgradeService;
        this.shardLimitValidator = shardLimitValidator;
        this.remoteClusterStateService = remoteClusterStateService;
    }

    /**
     * Restores data from remote store for indices specified in the restore request.
     *
     * @param request  restore request
     * @param listener restore listener
     */
    public void restore(RestoreRemoteStoreRequest request, final ActionListener<RestoreService.RestoreCompletionResponse> listener) {
        clusterService.submitStateUpdateTask("restore[remote_store]", new ClusterStateUpdateTask() {
            final String restoreUUID = UUIDs.randomBase64UUID();
            RestoreInfo restoreInfo = null;

            private ClusterState executeRestore(
                ClusterState currentState,
                Map<String, Tuple<Boolean, IndexMetadata>> indexMetadataMap,
                boolean restoreAllShards
            ) {
                List<String> indicesToBeRestored = new ArrayList<>();
                int totalShards = 0;
                ClusterState.Builder builder = ClusterState.builder(currentState);
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
                for (Map.Entry<String, Tuple<Boolean, IndexMetadata>> indexMetadataEntry : indexMetadataMap.entrySet()) {
                    String indexName = indexMetadataEntry.getKey();
                    IndexMetadata indexMetadata = indexMetadataEntry.getValue().v2();
                    boolean restoreMetadataFromRemote = indexMetadataEntry.getValue().v1();
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
                    } else if (restoreMetadataFromRemote == false) {
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
                Map<String, Tuple<Boolean, IndexMetadata>> indexMetadataMap = new HashMap<>();
                boolean metadataFromRemoteStore = (request.clusterUUID() == null
                    || request.clusterUUID().isEmpty()
                    || request.clusterUUID().isBlank()) == false;
                if (metadataFromRemoteStore) {
                    try {
                        for (Map.Entry<String, IndexMetadata> entry : remoteClusterStateService.getLatestIndexMetadata(
                            request.clusterUUID(),
                            currentState.getClusterName().value()
                        ).entrySet()) {
                            indexMetadataMap.put(entry.getKey(), new Tuple<>(true, entry.getValue()));
                        }
                    } catch (IOException e) {
                        throw new IllegalStateException("failed to download remote index metadata");
                    }
                } else {
                    for (String indexName : request.indices()) {
                        IndexMetadata indexMetadata = currentState.metadata().index(indexName);
                        if (indexMetadata == null) {
                            logger.warn("Index restore is not supported for non-existent index. Skipping: {}", indexName);
                        } else {
                            indexMetadataMap.put(indexName, new Tuple<>(false, indexMetadata));
                        }
                    }
                }
                validate(currentState, indexMetadataMap, request.restoreAllShards());
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
                listener.onResponse(new RestoreService.RestoreCompletionResponse(restoreUUID, null, restoreInfo));
            }
        });

    }

    private void validate(
        ClusterState currentState,
        Map<String, Tuple<Boolean, IndexMetadata>> indexMetadataMap,
        boolean restoreAllShards
    ) {
        String errorMsg = "cannot restore index [%s] because an open index with same name already exists in the cluster.";
        for (Map.Entry<String, Tuple<Boolean, IndexMetadata>> indexMetadataEntry : indexMetadataMap.entrySet()) {
            String indexName = indexMetadataEntry.getKey();
            IndexMetadata indexMetadata = indexMetadataEntry.getValue().v2();
            String indexUuid = indexMetadata.getIndexUUID();
            boolean fromRemoteStore = indexMetadataEntry.getValue().v1();
            if (indexMetadata.getSettings().getAsBoolean(SETTING_REMOTE_STORE_ENABLED, false)) {
                if (restoreAllShards && IndexMetadata.State.CLOSE.equals(indexMetadata.getState()) == false) {
                    throw new IllegalStateException(String.format(Locale.ROOT, errorMsg, indexName) + " Close the existing index.");
                }

                if (fromRemoteStore) {
                    boolean sameNameIndexExists = currentState.metadata().hasIndex(indexName);
                    boolean sameUUIDIndexExists = currentState.metadata()
                        .indices()
                        .values()
                        .stream()
                        .anyMatch(indMd -> indMd.isSameUUID(indexUuid));
                    if (sameNameIndexExists || sameUUIDIndexExists) {
                        throw new IllegalStateException(String.format(Locale.ROOT, errorMsg, indexName));
                    }
                    Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion().minimumIndexCompatibilityVersion();
                    metadataIndexUpgradeService.upgradeIndexMetadata(indexMetadata, minIndexCompatibilityVersion);
                    boolean isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(indexMetadata.getSettings());
                    createIndexService.validateIndexName(indexName, currentState);
                    createIndexService.validateDotIndex(indexName, isHidden);
                    createIndexService.validateIndexSettings(indexName, indexMetadata.getSettings(), false);
                    shardLimitValidator.validateShardLimit(indexName, indexMetadata.getSettings(), currentState);
                }
            } else {
                logger.warn("Remote store is not enabled for index: {}", indexName);
            }
        }
    }
}
