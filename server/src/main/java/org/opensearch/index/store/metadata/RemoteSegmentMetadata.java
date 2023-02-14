/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.metadata;

import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.index.store.RemoteSegmentStoreDirectory;

public class RemoteSegmentMetadata {
    private final Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata;

    public RemoteSegmentMetadata(Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata) {
        this.metadata = metadata;
    }

    public Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> getMetadata() {
        return this.metadata;
    }

    public static RemoteSegmentMetadata fromMapOfStrings(Map<String, String> segmentMetadata) {
        return new RemoteSegmentMetadata(
            segmentMetadata.entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> RemoteSegmentStoreDirectory.UploadedSegmentMetadata.fromString(entry.getValue())
                    )
                )
        );
    }

    /**
     * Ideally we shouldn't need expose internal data structures. all operations should be added into this class
     * @return
     */
    public Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> getMap() {
        return this.metadata;
    }
}
