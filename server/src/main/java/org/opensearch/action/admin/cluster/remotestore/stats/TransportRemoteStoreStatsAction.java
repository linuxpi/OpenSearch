/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.PlainShardsIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.index.IndexService;
import org.opensearch.index.RemoteSegmentUploadShardStatsTracker;
import org.opensearch.index.RemoteUploadStatsTracker;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 *
 */
public class TransportRemoteStoreStatsAction extends TransportBroadcastByNodeAction<
    RemoteStoreStatsRequest,
    RemoteStoreStatsResponse,
    RemoteStoreStats> {

    private final IndicesService indicesService;

    @Inject
    public TransportRemoteStoreStatsAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            RemoteStoreStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            RemoteStoreStatsRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
    }

    /**
     * Status goes across *all* shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, RemoteStoreStatsRequest request, String[] concreteIndices) {
        final List<ShardRouting> newShardRoutings = new ArrayList<>();
        if (request.getShards().length > 0) {
            clusterState.routingTable().allShards(concreteIndices).getShardRoutings().forEach(shardRouting -> {
                if (Arrays.asList(request.getShards()).contains(Integer.toString(shardRouting.shardId().id()))) {
                    newShardRoutings.add(shardRouting);
                }
            });
        } else {
            newShardRoutings.addAll(clusterState.routingTable().allShards(concreteIndices).getShardRoutings());
        }
        return new PlainShardsIterator(
            newShardRoutings.stream()
//                .filter(shardRouting -> shardRouting.currentNodeId() == null || shardRouting.currentNodeId().equals(clusterState.getNodes().getLocalNodeId()))
                .filter(ShardRouting::primary)
                .filter(shardRouting -> {
                    return indicesService.indexService(shardRouting.index()).getIndexSettings().isRemoteStoreEnabled();
//                    IndexShard indexShard = indicesService.getShardOrNull(shardRouting.shardId());
//                    return indexShard != null && indexShard.isRemoteStoreEnabled();
                })
                .collect(Collectors.toList())
        );
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, RemoteStoreStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, RemoteStoreStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected RemoteStoreStats readShardResult(StreamInput in) throws IOException {
        return new RemoteStoreStats(in);
    }

    @Override
    protected RemoteStoreStatsResponse newResponse(
        RemoteStoreStatsRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<RemoteStoreStats> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new RemoteStoreStatsResponse(
            responses.toArray(new RemoteStoreStats[0]),
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );
    }

    @Override
    protected RemoteStoreStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new RemoteStoreStatsRequest(in);
    }

    @Override
    protected RemoteStoreStats shardOperation(RemoteStoreStatsRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        // if we don't have the routing entry yet, we need it stats wise, we treat it as if the shard is not ready yet
        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }

        RemoteSegmentUploadShardStatsTracker remoteSegmentUploadShardStatsTracker = RemoteUploadStatsTracker.INSTANCE.getStatsTracker(
            indexShard.shardId()
        );
        assert Objects.nonNull(remoteSegmentUploadShardStatsTracker);

        return new RemoteStoreStats(remoteSegmentUploadShardStatsTracker);
    }
}
