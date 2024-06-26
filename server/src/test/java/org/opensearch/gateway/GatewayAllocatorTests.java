/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.snapshots.SnapshotShardSizeInfo;
import org.opensearch.test.gateway.TestShardBatchGatewayAllocator;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GatewayAllocatorTests extends OpenSearchAllocationTestCase {

    private final Logger logger = LogManager.getLogger(GatewayAllocatorTests.class);
    TestShardBatchGatewayAllocator testShardsBatchGatewayAllocator = null;
    ClusterState clusterState = null;
    RoutingAllocation testAllocation = null;
    String indexPrefix = "TEST";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        testShardsBatchGatewayAllocator = new TestShardBatchGatewayAllocator();
    }

    public void testSingleBatchCreation() {
        createIndexAndUpdateClusterState(1, 3, 1);
        createBatchesAndAssert(1);
    }

    public void testTwoBatchCreation() {
        createIndexAndUpdateClusterState(2, 1020, 1);
        createBatchesAndAssert(2);

        List<ShardsBatchGatewayAllocator.ShardsBatch> listOfBatches = new ArrayList<>(
            testShardsBatchGatewayAllocator.getBatchIdToStartedShardBatch().values()
        );
        assertNotEquals(listOfBatches.get(0), listOfBatches.get(1));

        // test for replicas
        listOfBatches = new ArrayList<>(testShardsBatchGatewayAllocator.getBatchIdToStoreShardBatch().values());
        assertNotEquals(listOfBatches.get(0), listOfBatches.get(1));
    }

    public void testNonDuplicationOfBatch() {
        createIndexAndUpdateClusterState(1, 3, 1);
        Tuple<Set<String>, Set<String>> batches = createBatchesAndAssert(1);
        assertEquals(1, batches.v1().size());
        assertEquals(1, batches.v2().size());

        // again try to create batch and verify no new batch is created since shard is already batched and no new unassigned shard
        assertEquals(batches.v1(), testShardsBatchGatewayAllocator.createAndUpdateBatches(testAllocation, true));
        assertEquals(batches.v2(), testShardsBatchGatewayAllocator.createAndUpdateBatches(testAllocation, false));
    }

    public void testCorrectnessOfBatch() {
        createIndexAndUpdateClusterState(2, 1020, 1);
        createBatchesAndAssert(2);
        Set<ShardId> shardsSet1 = clusterState.routingTable()
            .index(indexPrefix + 0)
            .getShards()
            .values()
            .stream()
            .map(IndexShardRoutingTable::getShardId)
            .collect(Collectors.toSet());
        Set<ShardId> shardsSet2 = clusterState.routingTable()
            .index(indexPrefix + 1)
            .getShards()
            .values()
            .stream()
            .map(IndexShardRoutingTable::getShardId)
            .collect(Collectors.toSet());
        shardsSet1.addAll(shardsSet2);

        Set<ShardId> shardsInAllbatches = testShardsBatchGatewayAllocator.getBatchIdToStartedShardBatch()
            .values()
            .stream()
            .map(ShardsBatchGatewayAllocator.ShardsBatch::getBatchedShards)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
        assertEquals(shardsInAllbatches, shardsSet1);
        shardsInAllbatches = testShardsBatchGatewayAllocator.getBatchIdToStoreShardBatch()
            .values()
            .stream()
            .map(ShardsBatchGatewayAllocator.ShardsBatch::getBatchedShards)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
        assertEquals(shardsInAllbatches, shardsSet1);

        Set<ShardRouting> primariesInAllBatches = testShardsBatchGatewayAllocator.getBatchIdToStartedShardBatch()
            .values()
            .stream()
            .map(ShardsBatchGatewayAllocator.ShardsBatch::getBatchedShardRoutings)
            .flatMap(List::stream)
            .collect(Collectors.toSet());
        primariesInAllBatches.forEach(shardRouting -> assertTrue(shardRouting.unassigned() && shardRouting.primary() == true));

        Set<ShardRouting> replicasInAllBatches = testShardsBatchGatewayAllocator.getBatchIdToStoreShardBatch()
            .values()
            .stream()
            .map(ShardsBatchGatewayAllocator.ShardsBatch::getBatchedShardRoutings)
            .flatMap(List::stream)
            .collect(Collectors.toSet());

        replicasInAllBatches.forEach(shardRouting -> assertTrue(shardRouting.unassigned() && shardRouting.primary() == false));
    }

    public void testAsyncFetcherCreationInBatch() {
        createIndexAndUpdateClusterState(1, 3, 1);
        Tuple<Set<String>, Set<String>> batchesTuple = createBatchesAndAssert(1);
        Set<String> primaryBatches = batchesTuple.v1();
        Set<String> replicaBatches = batchesTuple.v2();

        ShardsBatchGatewayAllocator.ShardsBatch shardsBatch = testShardsBatchGatewayAllocator.getBatchIdToStartedShardBatch()
            .get(primaryBatches.iterator().next());
        AsyncShardFetch<? extends BaseNodeResponse> asyncFetcher = shardsBatch.getAsyncFetcher();
        // assert asyncFetcher is not null
        assertNotNull(asyncFetcher);
        shardsBatch = testShardsBatchGatewayAllocator.getBatchIdToStoreShardBatch().get(replicaBatches.iterator().next());
        asyncFetcher = shardsBatch.getAsyncFetcher();
        assertNotNull(asyncFetcher);
    }

    public void testSafelyRemoveShardFromBatch() {
        createIndexAndUpdateClusterState(2, 1023, 1);

        Tuple<Set<String>, Set<String>> batchesTuple = createBatchesAndAssert(2);
        Set<String> primaryBatches = batchesTuple.v1();
        Set<String> replicaBatches = batchesTuple.v2();

        ShardsBatchGatewayAllocator.ShardsBatch primaryShardsBatch = testShardsBatchGatewayAllocator.getBatchIdToStartedShardBatch()
            .get(primaryBatches.iterator().next());
        ShardRouting primaryShardRouting = primaryShardsBatch.getBatchedShardRoutings().iterator().next();
        assertEquals(2, replicaBatches.size());
        ShardsBatchGatewayAllocator.ShardsBatch replicaShardsBatch = testShardsBatchGatewayAllocator.getBatchIdToStoreShardBatch()
            .get(replicaBatches.iterator().next());
        ShardRouting replicaShardRouting = replicaShardsBatch.getBatchedShardRoutings().iterator().next();

        // delete 1 shard routing from each batch
        testShardsBatchGatewayAllocator.safelyRemoveShardFromBatch(primaryShardRouting);

        testShardsBatchGatewayAllocator.safelyRemoveShardFromBatch(replicaShardRouting);
        // verify that shard routing is removed from both batches
        assertFalse(primaryShardsBatch.getBatchedShards().contains(primaryShardRouting.shardId()));
        assertFalse(replicaShardsBatch.getBatchedShards().contains(replicaShardRouting.shardId()));

        // try to remove that shard again to see if its no op and doent result in exception
        testShardsBatchGatewayAllocator.safelyRemoveShardFromBatch(primaryShardRouting);
        testShardsBatchGatewayAllocator.safelyRemoveShardFromBatch(replicaShardRouting);

        // now remove all shard routings to verify that batch only gets deleted
        primaryShardsBatch.getBatchedShardRoutings().forEach(testShardsBatchGatewayAllocator::safelyRemoveShardFromBatch);
        replicaShardsBatch.getBatchedShardRoutings().forEach(testShardsBatchGatewayAllocator::safelyRemoveShardFromBatch);

        assertFalse(testShardsBatchGatewayAllocator.getBatchIdToStartedShardBatch().containsKey(primaryShardsBatch.getBatchId()));
        assertFalse(testShardsBatchGatewayAllocator.getBatchIdToStoreShardBatch().containsKey(replicaShardsBatch.getBatchId()));
        assertEquals(1, testShardsBatchGatewayAllocator.getBatchIdToStartedShardBatch().size());
        assertEquals(1, testShardsBatchGatewayAllocator.getBatchIdToStoreShardBatch().size());
    }

    public void testSafelyRemoveShardFromBothBatch() {
        createIndexAndUpdateClusterState(1, 3, 1);
        createBatchesAndAssert(1);
        ShardsBatchGatewayAllocator.ShardsBatch primaryShardsBatch = testShardsBatchGatewayAllocator.getBatchIdToStartedShardBatch()
            .values()
            .iterator()
            .next();
        ShardsBatchGatewayAllocator.ShardsBatch replicaShardsBatch = testShardsBatchGatewayAllocator.getBatchIdToStoreShardBatch()
            .values()
            .iterator()
            .next();

        ShardRouting anyPrimary = primaryShardsBatch.getBatchedShardRoutings().iterator().next();
        // remove first shard routing from both batches
        testShardsBatchGatewayAllocator.safelyRemoveShardFromBothBatch(anyPrimary);

        // verify that shard routing is removed from both batches
        assertFalse(primaryShardsBatch.getBatchedShards().contains(anyPrimary.shardId()));
        assertFalse(replicaShardsBatch.getBatchedShards().contains(anyPrimary.shardId()));

        // try to remove that shard again to see if its no op and doesnt result in exception
        testShardsBatchGatewayAllocator.safelyRemoveShardFromBothBatch(anyPrimary);

        // now remove all shard routings to verify that batch gets deleted
        primaryShardsBatch.getBatchedShardRoutings().forEach(testShardsBatchGatewayAllocator::safelyRemoveShardFromBothBatch);
        replicaShardsBatch.getBatchedShardRoutings().forEach(testShardsBatchGatewayAllocator::safelyRemoveShardFromBothBatch);

        assertFalse(testShardsBatchGatewayAllocator.getBatchIdToStartedShardBatch().containsKey(primaryShardsBatch.getBatchId()));
        assertFalse(testShardsBatchGatewayAllocator.getBatchIdToStoreShardBatch().containsKey(replicaShardsBatch.getBatchId()));
        assertEquals(0, testShardsBatchGatewayAllocator.getBatchIdToStartedShardBatch().size());
        assertEquals(0, testShardsBatchGatewayAllocator.getBatchIdToStoreShardBatch().size());
    }

    public void testGetBatchIdExisting() {
        createIndexAndUpdateClusterState(2, 1020, 1);
        // get all shardsRoutings for test index
        List<ShardRouting> allShardRoutings1 = clusterState.routingTable()
            .index(indexPrefix + 0)
            .getShards()
            .values()
            .stream()
            .map(IndexShardRoutingTable::getShards)
            .flatMap(List::stream)
            .collect(Collectors.toList());
        List<ShardRouting> allShardRouting2 = clusterState.routingTable()
            .index(indexPrefix + 1)
            .getShards()
            .values()
            .stream()
            .map(IndexShardRoutingTable::getShards)
            .flatMap(List::stream)
            .collect(Collectors.toList());

        Tuple<Set<String>, Set<String>> batchesTuple = createBatchesAndAssert(2);
        Set<String> primaryBatches = batchesTuple.v1();
        Set<String> replicaBatches = batchesTuple.v2();

        // create a map of shards to batch id for primaries

        Map<ShardId, String> shardIdToBatchIdForStartedShards = new HashMap<>();
        allShardRoutings1.addAll(allShardRouting2);
        assertEquals(4080, allShardRoutings1.size());
        for (ShardRouting shardRouting : allShardRoutings1) {
            for (String batchId : primaryBatches) {
                if (shardRouting.primary() == true
                    && testShardsBatchGatewayAllocator.getBatchIdToStartedShardBatch()
                        .get(batchId)
                        .getBatchedShards()
                        .contains(shardRouting.shardId())) {
                    if (shardIdToBatchIdForStartedShards.containsKey(shardRouting.shardId())) {
                        fail("found duplicate shard routing for shard. One shard cant be in multiple batches " + shardRouting.shardId());
                    }
                    assertTrue(shardRouting.primary());
                    shardIdToBatchIdForStartedShards.put(shardRouting.shardId(), batchId);
                }
            }
        }
        Map<ShardId, String> shardIdToBatchIdForStoreShards = new HashMap<>();

        for (ShardRouting shardRouting : allShardRoutings1) {
            for (String batchId : replicaBatches) {
                if (shardRouting.primary() == false
                    && testShardsBatchGatewayAllocator.getBatchIdToStoreShardBatch()
                        .get(batchId)
                        .getBatchedShards()
                        .contains(shardRouting.shardId())) {
                    if (shardIdToBatchIdForStoreShards.containsKey(shardRouting.shardId())) {
                        fail("found duplicate shard routing for shard. One shard cant be in multiple batches " + shardRouting.shardId());
                    }
                    assertFalse(shardRouting.primary());
                    shardIdToBatchIdForStoreShards.put(shardRouting.shardId(), batchId);
                }
            }
        }

        assertEquals(4080, shardIdToBatchIdForStartedShards.size() + shardIdToBatchIdForStoreShards.size());
        // now compare the maps with getBatchId() call
        for (ShardRouting shardRouting : allShardRoutings1) {
            if (shardRouting.primary()) {
                assertEquals(
                    shardIdToBatchIdForStartedShards.get(shardRouting.shardId()),
                    testShardsBatchGatewayAllocator.getBatchId(shardRouting, true)
                );
            } else {
                assertEquals(
                    shardIdToBatchIdForStoreShards.get(shardRouting.shardId()),
                    testShardsBatchGatewayAllocator.getBatchId(shardRouting, false)
                );
            }
        }
    }

    public void testGetBatchIdNonExisting() {
        createIndexAndUpdateClusterState(1, 1, 1);
        List<ShardRouting> allShardRoutings = clusterState.routingTable()
            .index(indexPrefix + 0)
            .getShards()
            .values()
            .stream()
            .map(IndexShardRoutingTable::getShards)
            .flatMap(List::stream)
            .collect(Collectors.toList());
        allShardRoutings.forEach(shard -> assertNull(testShardsBatchGatewayAllocator.getBatchId(shard, shard.primary())));
    }

    private void createIndexAndUpdateClusterState(int count, int numberOfShards, int numberOfReplicas) {
        if (count == 0) return;
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (int i = 0; i < count; i++) {
            String indexName = indexPrefix + i;
            metadata.put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            );
        }
        for (int i = 0; i < count; i++) {
            String indexName = indexPrefix + i;
            routingTableBuilder = routingTableBuilder.addAsNew(metadata.build().index(indexName));
        }
        clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata.build())
            .routingTable(routingTableBuilder.build())
            .build();
        testAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.emptyList()),
            new RoutingNodes(clusterState, false),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    // call this after index creation and update cluster state
    private Tuple<Set<String>, Set<String>> createBatchesAndAssert(int expectedBatchSize) {
        Set<String> primaryBatches = testShardsBatchGatewayAllocator.createAndUpdateBatches(testAllocation, true);
        Set<String> replicaBatches = testShardsBatchGatewayAllocator.createAndUpdateBatches(testAllocation, false);
        assertEquals(expectedBatchSize, primaryBatches.size());
        assertEquals(expectedBatchSize, replicaBatches.size());
        assertEquals(expectedBatchSize, testShardsBatchGatewayAllocator.getBatchIdToStartedShardBatch().size());
        assertEquals(expectedBatchSize, testShardsBatchGatewayAllocator.getBatchIdToStoreShardBatch().size());
        assertEquals(testShardsBatchGatewayAllocator.getBatchIdToStartedShardBatch().keySet(), primaryBatches);
        assertEquals(testShardsBatchGatewayAllocator.getBatchIdToStoreShardBatch().keySet(), replicaBatches);
        return new Tuple<>(primaryBatches, replicaBatches);
    }
}
