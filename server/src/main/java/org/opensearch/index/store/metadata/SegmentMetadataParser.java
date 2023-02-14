/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.metadata;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class SegmentMetadataParser implements MetadataParser<RemoteSegmentMetadata> {
    @Override
    public RemoteSegmentMetadata readContent(IndexInput indexInput) throws IOException {
        return RemoteSegmentMetadata.fromMapOfStrings(indexInput.readMapOfStrings());
    }

    @Override
    public void writeContent(IndexOutput indexOutput, RemoteSegmentMetadata content) throws IOException {}

    @Override
    public void writeContent(IndexOutput indexOutput, Map<String, String> content) throws IOException {
        indexOutput.writeMapOfStrings(content);
    }
}
