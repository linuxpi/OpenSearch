/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;


import org.opensearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.unit.TimeValue;

public class RemoteStoreStatsRequestBuilder extends BroadcastOperationRequestBuilder<RemoteStoreStatsRequest, RemoteStoreStatsResponse, RemoteStoreStatsRequestBuilder> {

    public RemoteStoreStatsRequestBuilder(OpenSearchClient client, RemoteStoreStatsAction action) {
        super(client, action, new RemoteStoreStatsRequest());
    }

    /**
     * Sets all flags to return all stats.
     */
    public RemoteStoreStatsRequestBuilder all() {
        request.all();
        return this;
    }

    /**
     * Clears all stats.
     */
    public RemoteStoreStatsRequestBuilder clear() {
        request.clear();
        return this;
    }

    /**
     * Sets timeout of request.
     */
    public final RemoteStoreStatsRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setGroups(String... groups) {
        request.groups(groups);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setDocs(boolean docs) {
        request.docs(docs);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setStore(boolean store) {
        request.store(store);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setIndexing(boolean indexing) {
        request.indexing(indexing);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setGet(boolean get) {
        request.get(get);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setSearch(boolean search) {
        request.search(search);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setMerge(boolean merge) {
        request.merge(merge);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setRefresh(boolean refresh) {
        request.refresh(refresh);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setFlush(boolean flush) {
        request.flush(flush);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setWarmer(boolean warmer) {
        request.warmer(warmer);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setQueryCache(boolean queryCache) {
        request.queryCache(queryCache);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setFieldData(boolean fieldData) {
        request.fieldData(fieldData);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setFieldDataFields(String... fields) {
        request.fieldDataFields(fields);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setSegments(boolean segments) {
        request.segments(segments);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setCompletion(boolean completion) {
        request.completion(completion);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setCompletionFields(String... fields) {
        request.completionFields(fields);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setTranslog(boolean translog) {
        request.translog(translog);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setRequestCache(boolean requestCache) {
        request.requestCache(requestCache);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setRecovery(boolean recovery) {
        request.recovery(recovery);
        return this;
    }

    public RemoteStoreStatsRequestBuilder setIncludeSegmentFileSizes(boolean includeSegmentFileSizes) {
        request.includeSegmentFileSizes(includeSegmentFileSizes);
        return this;
    }
}
