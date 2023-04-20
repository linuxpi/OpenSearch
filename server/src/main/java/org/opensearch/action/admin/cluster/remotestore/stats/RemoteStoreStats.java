/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.RemoteRefreshSegmentPressureTracker;

import java.io.IOException;

/**
 * Do we need this stats wrapper
 */
public class RemoteStoreStats implements Writeable, ToXContentFragment {

    private RemoteRefreshSegmentPressureTracker.Stats remoteSegmentUploadShardStatsTracker;

    public RemoteStoreStats(RemoteRefreshSegmentPressureTracker.Stats remoteSegmentUploadShardStatsTracker) {
        this.remoteSegmentUploadShardStatsTracker = remoteSegmentUploadShardStatsTracker;
    }

    public RemoteStoreStats(StreamInput in) {
        try {
            remoteSegmentUploadShardStatsTracker = in.readOptionalWriteable(RemoteRefreshSegmentPressureTracker.Stats::new);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public RemoteRefreshSegmentPressureTracker.Stats getStats() {
        return remoteSegmentUploadShardStatsTracker;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("shardId", remoteSegmentUploadShardStatsTracker.shardId)
            .field("local_refresh_time", remoteSegmentUploadShardStatsTracker.localRefreshTime)
            .field("local_refresh_seq_no", remoteSegmentUploadShardStatsTracker.localRefreshSeqNo)
            .field("remote_refresh_time", remoteSegmentUploadShardStatsTracker.remoteRefreshTime)
            .field("remote_refresh_seqno", remoteSegmentUploadShardStatsTracker.remoteRefreshSeqNo)
            .field("upload_bytes_started", remoteSegmentUploadShardStatsTracker.uploadBytesStarted)
            .field("upload_bytes_succeeded", remoteSegmentUploadShardStatsTracker.uploadBytesSucceeded)
            .field("upload_bytes_failed", remoteSegmentUploadShardStatsTracker.uploadBytesFailed)
            .field("total_upload_started", remoteSegmentUploadShardStatsTracker.totalUploadsStarted)
            .field("total_upload_succeeded", remoteSegmentUploadShardStatsTracker.totalUploadsSucceeded)
            .field("total_upload_failed", remoteSegmentUploadShardStatsTracker.totalUploadsFailed)
            .field("upload_time_average", remoteSegmentUploadShardStatsTracker.uploadTimeMovingAverage)
            .field("upload_bytes_per_sec_average", remoteSegmentUploadShardStatsTracker.uploadBytesPerSecMovingAverage)
            .field("upload_bytes_average", remoteSegmentUploadShardStatsTracker.uploadBytesMovingAverage)
            .field("bytes_lag", remoteSegmentUploadShardStatsTracker.bytesLag)
            .field("inflight_upload_bytes", remoteSegmentUploadShardStatsTracker.inflightUploadBytes)
            .field("inflight_uploads", remoteSegmentUploadShardStatsTracker.inflightUploads)
            .field("rejection_count", remoteSegmentUploadShardStatsTracker.rejectionCount)
            .endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(remoteSegmentUploadShardStatsTracker);
    }
}
