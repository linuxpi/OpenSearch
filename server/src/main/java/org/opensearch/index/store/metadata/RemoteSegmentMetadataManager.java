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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class RemoteSegmentMetadataManager {
    static final int CURRENT_VERSION = 1;
    static final String METADATA_CODEC = "segment_md";

    // replace with a parser factory to support multiple versions of metadata in future if needed
    private final MetadataParser<RemoteSegmentMetadata> parser;

    public RemoteSegmentMetadataManager(MetadataParser<RemoteSegmentMetadata> parser) {
        this.parser = parser;
    }

    public RemoteSegmentMetadata readMetadata(IndexInput indexInput) throws IOException {
        ChecksumIndexInput checksumIndexInput = new BufferedChecksumIndexInput(indexInput);
        checkHeader(checksumIndexInput);
        RemoteSegmentMetadata metadata = this.parser.readContent(checksumIndexInput);
        checkFooter(checksumIndexInput);
        return metadata;
    }

    /**
     * this method should only accept RemoteSegmentMetadata and not Map<String, String>
     * @param indexOutput
     * @param metadata
     * @throws IOException
     */
    public void writeMetadata(IndexOutput indexOutput, Map<String, String> metadata) throws IOException {
        this.writeHeader(indexOutput);
        this.parser.writeContent(indexOutput, metadata);
        this.writeFooter(indexOutput);
    }

    private int checkHeader(IndexInput indexInput) throws IOException {
        return CodecUtil.checkHeader(indexInput, METADATA_CODEC, CURRENT_VERSION, CURRENT_VERSION);
    }

    private void checkFooter(ChecksumIndexInput indexInput) throws IOException {
        CodecUtil.checkFooter(indexInput);
    }

    private void writeHeader(IndexOutput indexOutput) throws IOException {
        CodecUtil.writeHeader(indexOutput, METADATA_CODEC, CURRENT_VERSION);
    }

    private void writeFooter(IndexOutput indexOutput) throws IOException {
        CodecUtil.writeFooter(indexOutput);
    }
}
