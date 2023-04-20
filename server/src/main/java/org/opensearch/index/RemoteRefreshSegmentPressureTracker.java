/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.common.util.Streak;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Keeps track of remote refresh which happens in {@link org.opensearch.index.shard.RemoteStoreRefreshListener}. This consist of multiple critical metrics.
 */
public class RemoteRefreshSegmentPressureTracker {

    RemoteRefreshSegmentPressureTracker(ShardId shardId, RemoteRefreshSegmentPressureSettings remoteUploadPressureSettings) {
        this.shardId = shardId;
        // Both the local refresh time and remote refresh time are set with current time to give consistent view of time lag when it arises.
        long currentTimeMs = System.nanoTime() / 1_000_000L;
        localRefreshTimeMs.set(currentTimeMs);
        remoteRefreshTimeMs.set(currentTimeMs);
        uploadBytesMovingAverageReference = new AtomicReference<>(
            new MovingAverage(remoteUploadPressureSettings.getUploadBytesMovingAverageWindowSize())
        );
        uploadBytesPerSecMovingAverageReference = new AtomicReference<>(
            new MovingAverage(remoteUploadPressureSettings.getUploadBytesPerSecMovingAverageWindowSize())
        );
        uploadTimeMsMovingAverageReference = new AtomicReference<>(
            new MovingAverage(remoteUploadPressureSettings.getUploadTimeMovingAverageWindowSize())
        );
    }

    /**
     * ShardId for which this instance tracks the remote segment upload metadata.
     */
    private final ShardId shardId;

    /**
     * Every refresh is assigned a sequence number. This is the sequence number of the most recent refresh.
     */
    private final AtomicLong localRefreshSeqNo = new AtomicLong();

    /**
     * The refresh time of the most recent refresh.
     */
    private final AtomicLong localRefreshTimeMs = new AtomicLong();

    /**
     * Sequence number of the most recent remote refresh.
     */
    private final AtomicLong remoteRefreshSeqNo = new AtomicLong();

    /**
     * The refresh time of most recent remote refresh.
     */
    private final AtomicLong remoteRefreshTimeMs = new AtomicLong();

    /**
     * Keeps the seq no lag computed so that we do not compute it for every request.
     */
    private final AtomicLong seqNoLag = new AtomicLong();

    /**
     * Keeps the time (ms) lag computed so that we do not compute it for every request.
     */
    private final AtomicLong timeMsLag = new AtomicLong();

    /**
     * Cumulative sum of size in bytes of segment files for which upload has started during remote refresh.
     */
    private final AtomicLong uploadBytesStarted = new AtomicLong();

    /**
     * Cumulative sum of size in bytes of segment files for which upload has failed during remote refresh.
     */
    private final AtomicLong uploadBytesFailed = new AtomicLong();

    /**
     * Cumulative sum of size in bytes of segment files for which upload has succeeded during remote refresh.
     */
    private final AtomicLong uploadBytesSucceeded = new AtomicLong();

    /**
     * Cumulative sum of count of remote refreshes that have started.
     */
    private final AtomicLong totalUploadsStarted = new AtomicLong();

    /**
     * Cumulative sum of count of remote refreshes that have failed.
     */
    private final AtomicLong totalUploadsFailed = new AtomicLong();

    /**
     * Cumulative sum of count of remote refreshes that have succeeded.
     */
    private final AtomicLong totalUploadsSucceeded = new AtomicLong();

    /**
     * Cumulative sum of rejection counts for this shard.
     */
    private final AtomicLong rejectionCount = new AtomicLong();

    /**
     * Map of name to size of the segment files created as part of the most recent refresh.
     */
    private volatile Map<String, Long> latestLocalFileNameLengthMap;

    /**
     * Set of names of segment files that were uploaded as part of the most recent remote refresh.
     */
    private final Set<String> latestUploadFiles = ConcurrentCollections.newConcurrentSet();

    /**
     * Keeps the bytes lag computed so that we do not compute it for every request.
     */
    private final AtomicLong bytesLag = new AtomicLong();

    /**
     * Holds count of consecutive failures until last success. Gets reset to zero if there is a success.
     */
    private final Streak failures = new Streak();

    /**
     * Provides moving average over the last N total size in bytes of segment files uploaded as part of remote refresh.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> uploadBytesMovingAverageReference;

    /**
     * Provides moving average over the last N upload speed (in bytes/s) of segment files uploaded as part of remote refresh.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> uploadBytesPerSecMovingAverageReference;

    /**
     * Provides moving average over the last N overall upload time (in nanos) as part of remote refresh.N is window size.
     * Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> uploadTimeMsMovingAverageReference;

    ShardId getShardId() {
        return shardId;
    }

    long getLocalRefreshSeqNo() {
        return localRefreshSeqNo.get();
    }

    void updateLocalRefreshSeqNo(long localRefreshSeqNo) {
        assert localRefreshSeqNo > this.localRefreshSeqNo.get() : "newLocalRefreshSeqNo="
            + localRefreshSeqNo
            + ">="
            + "currentLocalRefreshSeqNo="
            + this.localRefreshSeqNo.get();
        this.localRefreshSeqNo.set(localRefreshSeqNo);
        computeSeqNoLag();
    }

    long getLocalRefreshTimeMs() {
        return localRefreshTimeMs.get();
    }

    void updateLocalRefreshTimeMs(long localRefreshTimeMs) {
        assert localRefreshTimeMs > this.localRefreshTimeMs.get() : "newLocalRefreshTimeMs="
            + localRefreshTimeMs
            + ">="
            + "currentLocalRefreshTimeMs="
            + this.localRefreshTimeMs.get();
        this.localRefreshTimeMs.set(localRefreshTimeMs);
        computeTimeMsLag();
    }

    long getRemoteRefreshSeqNo() {
        return remoteRefreshSeqNo.get();
    }

    void updateRemoteRefreshSeqNo(long remoteRefreshSeqNo) {
        assert remoteRefreshSeqNo > this.remoteRefreshSeqNo.get() : "newRemoteRefreshSeqNo="
            + remoteRefreshSeqNo
            + ">="
            + "currentRemoteRefreshSeqNo="
            + this.remoteRefreshSeqNo.get();
        this.remoteRefreshSeqNo.set(remoteRefreshSeqNo);
        computeSeqNoLag();
    }

    long getRemoteRefreshTimeMs() {
        return remoteRefreshTimeMs.get();
    }

    void updateRemoteRefreshTimeMs(long remoteRefreshTimeMs) {
        assert remoteRefreshTimeMs > this.remoteRefreshTimeMs.get() : "newRemoteRefreshTimeMs="
            + remoteRefreshTimeMs
            + ">="
            + "currentRemoteRefreshTimeMs="
            + this.remoteRefreshTimeMs.get();
        this.remoteRefreshTimeMs.set(remoteRefreshTimeMs);
        computeTimeMsLag();
    }

    private void computeSeqNoLag() {
        seqNoLag.set(localRefreshSeqNo.get() - remoteRefreshSeqNo.get());
    }

    long getSeqNoLag() {
        return seqNoLag.get();
    }

    private void computeTimeMsLag() {
        timeMsLag.set(localRefreshTimeMs.get() - remoteRefreshTimeMs.get());
    }

    long getTimeMsLag() {
        return timeMsLag.get();
    }

    long getBytesLag() {
        return bytesLag.get();
    }

    long getUploadBytesStarted() {
        return uploadBytesStarted.get();
    }

    void addUploadBytesStarted(long size) {
        uploadBytesStarted.addAndGet(size);
    }

    long getUploadBytesFailed() {
        return uploadBytesFailed.get();
    }

    void addUploadBytesFailed(long size) {
        uploadBytesFailed.addAndGet(size);
    }

    long getUploadBytesSucceeded() {
        return uploadBytesSucceeded.get();
    }

    void addUploadBytesSucceeded(long size) {
        uploadBytesSucceeded.addAndGet(size);
    }

    long getInflightUploadBytes() {
        return uploadBytesStarted.get() - uploadBytesFailed.get() - uploadBytesSucceeded.get();
    }

    long getTotalUploadsStarted() {
        return totalUploadsStarted.get();
    }

    void incrementTotalUploadsStarted() {
        totalUploadsStarted.incrementAndGet();
    }

    long getTotalUploadsFailed() {
        return totalUploadsFailed.get();
    }

    void incrementTotalUploadsFailed() {
        totalUploadsFailed.incrementAndGet();
        failures.record(true);
    }

    long getTotalUploadsSucceeded() {
        return totalUploadsSucceeded.get();
    }

    void incrementTotalUploadSucceeded() {
        totalUploadsSucceeded.incrementAndGet();
        failures.record(false);
    }

    long getInflightUploads() {
        return totalUploadsStarted.get() - totalUploadsFailed.get() - totalUploadsSucceeded.get();
    }

    long getRejectionCount() {
        return rejectionCount.get();
    }

    void incrementRejectionCount() {
        rejectionCount.incrementAndGet();
    }

    Map<String, Long> getLatestLocalFileNameLengthMap() {
        return latestLocalFileNameLengthMap;
    }

    void setLatestLocalFileNameLengthMap(Map<String, Long> latestLocalFileNameLengthMap) {
        this.latestLocalFileNameLengthMap = latestLocalFileNameLengthMap;
        computeBytesLag();
    }

    void addToLatestUploadFiles(String file) {
        this.latestUploadFiles.add(file);
        computeBytesLag();
    }

    private void computeBytesLag() {
        if (latestLocalFileNameLengthMap == null || latestLocalFileNameLengthMap.isEmpty()) {
            return;
        }
        Set<String> filesNotYetUploaded = latestLocalFileNameLengthMap.keySet()
            .stream()
            .filter(f -> !latestUploadFiles.contains(f))
            .collect(Collectors.toSet());
        long bytesLag = filesNotYetUploaded.stream().map(latestLocalFileNameLengthMap::get).mapToLong(Long::longValue).sum();
        this.bytesLag.set(bytesLag);
    }

    int getConsecutiveFailureCount() {
        return failures.length();
    }

    boolean isUploadBytesAverageReady() {
        return uploadBytesMovingAverageReference.get().isReady();
    }

    double getUploadBytesAverage() {
        return uploadBytesMovingAverageReference.get().getAverage();
    }

    void addUploadBytes(long size) {
        this.uploadBytesMovingAverageReference.get().record(size);
    }

    /**
     * Updates the window size for data collection of upload bytes. This also resets any data collected so far.
     *
     * @param updatedSize the updated size
     */
    void updateUploadBytesMovingAverageWindowSize(int updatedSize) {
        this.uploadBytesMovingAverageReference.set(new MovingAverage(updatedSize));
    }

    boolean isUploadBytesPerSecAverageReady() {
        return uploadBytesPerSecMovingAverageReference.get().isReady();
    }

    double getUploadBytesPerSecAverage() {
        return uploadBytesPerSecMovingAverageReference.get().getAverage();
    }

    void addUploadBytesPerSec(long bytesPerSec) {
        this.uploadBytesPerSecMovingAverageReference.get().record(bytesPerSec);
    }

    /**
     * Updates the window size for data collection of upload bytes per second. This also resets any data collected so far.
     *
     * @param updatedSize the updated size
     */
    void updateUploadBytesPerSecMovingAverageWindowSize(int updatedSize) {
        this.uploadBytesPerSecMovingAverageReference.set(new MovingAverage(updatedSize));
    }

    boolean isUploadTimeMsAverageReady() {
        return uploadTimeMsMovingAverageReference.get().isReady();
    }

    double getUploadTimeMsAverage() {
        return uploadTimeMsMovingAverageReference.get().getAverage();
    }

    void addUploadTimeMs(long timeMs) {
        this.uploadTimeMsMovingAverageReference.get().record(timeMs);
    }

    /**
     * Updates the window size for data collection of upload time (ms). This also resets any data collected so far.
     *
     * @param updatedSize the updated size
     */
    void updateUploadTimeMsMovingAverageWindowSize(int updatedSize) {
        this.uploadTimeMsMovingAverageReference.set(new MovingAverage(updatedSize));
    }

    public Stats stats() {
        return new Stats(shardId,
                         localRefreshTimeMs.get(),
                         localRefreshSeqNo.get(),
                         remoteRefreshTimeMs.get(),
                         remoteRefreshSeqNo.get(),
                         uploadBytesStarted.get(),
                         uploadBytesSucceeded.get(),
                         uploadBytesFailed.get(),
                         totalUploadsStarted.get(),
                         totalUploadsSucceeded.get(),
                         totalUploadsFailed.get(),
                         rejectionCount.get(),
                         failures.length(),
                         uploadBytesMovingAverageReference.get().getAverage(),
                         uploadBytesPerSecMovingAverageReference.get().getAverage(),
                         uploadTimeMsMovingAverageReference.get().getAverage(),
                         getBytesLag(),
                         getInflightUploads(),
                         getInflightUploadBytes());
    }

    public static class Stats implements Writeable {

        public ShardId shardId;
        public long localRefreshSeqNo;
        public long localRefreshTime;
        public long remoteRefreshSeqNo;
        public long remoteRefreshTime;
        public long uploadBytesStarted;
        public long uploadBytesFailed;
        public long uploadBytesSucceeded;
        public long totalUploadsStarted;
        public long totalUploadsFailed;
        public long totalUploadsSucceeded;
        public long rejectionCount;
        public long consecutiveFailuresCount;
        public double uploadBytesMovingAverage;
        public double uploadBytesPerSecMovingAverage;
        public double uploadTimeMovingAverage;
        public long bytesLag;
        public long inflightUploads;
        public long inflightUploadBytes;

        public Stats(ShardId shardId,
                     long localRefreshSeqNo,
                     long localRefreshTime,
                     long remoteRefreshSeqNo,
                     long remoteRefreshTime,
                     long uploadBytesStarted,
                     long uploadBytesSucceeded,
                     long uploadBytesFailed,
                     long totalUploadsStarted,
                     long totalUploadsSucceeded,
                     long totalUploadsFailed,
                     long rejectionCount,
                     long consecutiveFailuresCount,
                     double uploadBytesMovingAverage,
                     double uploadBytesPerSecMovingAverage,
                     double uploadTimeMovingAverage,
                     long bytesLag,
                     long inflightUploads,
                     long inflightUploadBytes) {
            this.shardId = shardId;
            this.localRefreshSeqNo = localRefreshSeqNo;
            this.localRefreshTime = localRefreshTime;
            this.remoteRefreshSeqNo = remoteRefreshSeqNo;
            this.remoteRefreshTime = remoteRefreshTime;
            this.uploadBytesStarted = uploadBytesStarted;
            this.uploadBytesFailed = uploadBytesFailed;
            this.uploadBytesSucceeded = uploadBytesSucceeded;
            this.totalUploadsStarted = totalUploadsStarted;
            this.totalUploadsFailed = totalUploadsFailed;
            this.totalUploadsSucceeded = totalUploadsSucceeded;
            this.rejectionCount = rejectionCount;
            this.consecutiveFailuresCount = consecutiveFailuresCount;
            this.uploadBytesMovingAverage = uploadBytesMovingAverage;
            this.uploadBytesPerSecMovingAverage = uploadBytesPerSecMovingAverage;
            this.uploadTimeMovingAverage = uploadTimeMovingAverage;
            this.bytesLag = bytesLag;
            this.inflightUploads = inflightUploads;
            this.inflightUploadBytes = inflightUploadBytes;
        }

        public Stats(StreamInput in) {
            try {
                this.shardId = new ShardId(in);
                this.localRefreshSeqNo = in.readLong();
                this.localRefreshTime = in.readLong();
                this.remoteRefreshSeqNo = in.readLong();
                this.remoteRefreshTime = in.readLong();
                this.uploadBytesStarted = in.readLong();
                this.uploadBytesFailed = in.readLong();
                this.uploadBytesSucceeded = in.readLong();
                this.totalUploadsStarted = in.readLong();
                this.totalUploadsFailed = in.readLong();
                this.totalUploadsSucceeded = in.readLong();
                this.rejectionCount = in.readLong();
                this.consecutiveFailuresCount = in.readLong();
                this.uploadBytesMovingAverage = in.readDouble();
                this.uploadBytesPerSecMovingAverage = in.readDouble();
                this.uploadTimeMovingAverage = in.readDouble();
                this.bytesLag = in.readLong();
                this.inflightUploads = in.readLong();
                this.inflightUploadBytes = in.readLong();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            out.writeLong(localRefreshSeqNo);
            out.writeLong(localRefreshTime);
            out.writeLong(remoteRefreshSeqNo);
            out.writeLong(remoteRefreshTime);
            out.writeLong(uploadBytesStarted);
            out.writeLong(uploadBytesFailed);
            out.writeLong(uploadBytesSucceeded);
            out.writeLong(totalUploadsStarted);
            out.writeLong(totalUploadsFailed);
            out.writeLong(totalUploadsSucceeded);
            out.writeLong(rejectionCount);
            out.writeLong(consecutiveFailuresCount);
            out.writeDouble(uploadBytesMovingAverage);
            out.writeDouble(uploadBytesPerSecMovingAverage);
            out.writeDouble(uploadTimeMovingAverage);
            out.writeLong(bytesLag);
            out.writeLong(inflightUploads);
            out.writeLong(inflightUploadBytes);
        }
    }

}
