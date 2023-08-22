package org.opensearch.cluster.store;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;

/**
 * Marker file which contains the details of the uploaded entity metadata
 *
 * @opensearch.internal
 */
public class ClusterMetadataMarker implements Writeable, ToXContentFragment {

    private static final ParseField INDICES_FIELD = new ParseField("indices");
    private static final ParseField TERM_FIELD = new ParseField("term");
    private static final ParseField VERSION_FIELD = new ParseField("version");
    private static final ParseField CLUSTER_UUID_FIELD = new ParseField("cluster_uuid");
    private static final ParseField STATE_UUID_FIELD = new ParseField("state_uuid");

    private static Map<String, UploadedIndexMetadata> indices(Object[] fields) {
        return (Map<String, UploadedIndexMetadata>) fields[0];
    }

    private static long term(Object[] fields) {
        return (long) fields[1];
    }

    private static long version(Object[] fields) {
        return (long) fields[2];
    }

    private static String clusterUUID(Object[] fields) {
        return (String) fields[3];
    }

    private static String stateUUID(Object[] fields) {
        return (String) fields[4];
    }

    private static final ConstructingObjectParser<ClusterMetadataMarker, Void> PARSER = new ConstructingObjectParser<>("cluster_metadata_marker",
        fields -> new ClusterMetadataMarker(indices(fields), term(fields), version(fields), clusterUUID(fields), stateUUID(fields)));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, UploadedIndexMetadata> uploadMetadataMap = new HashMap<>();
            while (p.nextToken() != Token.END_OBJECT) {
                UploadedIndexMetadata uploadMetadata = UploadedIndexMetadata.fromXContent(p);
                uploadMetadataMap.put(uploadMetadata.getIndexName(), uploadMetadata);
            }
            return uploadMetadataMap;
        }, INDICES_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TERM_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), CLUSTER_UUID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), STATE_UUID_FIELD);
    }

    private final Map<String, UploadedIndexMetadata> indices;
    private final long term;
    private final long version;
    private final String clusterUUID;
    private final String stateUUID;

    public Map<String, UploadedIndexMetadata> getIndices() {
        return indices;
    }

    public long getTerm() {
        return term;
    }

    public long getVersion() {
        return version;
    }

    public String getClusterUUID() {
        return clusterUUID;
    }

    public String getStateUUID() {
        return stateUUID;
    }

    public ClusterMetadataMarker(
        Map<String, UploadedIndexMetadata> indices,
        long term,
        long version,
        String clusterUUID,
        String stateUUID
    ) {
        this.indices = Collections.unmodifiableMap(indices);
        this.term = term;
        this.version = version;
        this.clusterUUID = clusterUUID;
        this.stateUUID = stateUUID;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(INDICES_FIELD.getPreferredName());
        {
            for (UploadedIndexMetadata uploadedIndexMetadata : indices.values()) {
                uploadedIndexMetadata.toXContent(builder, params);
            }
        }
        builder.endObject();
        builder.field(TERM_FIELD.getPreferredName(), getTerm()).field(VERSION_FIELD.getPreferredName(), getVersion())
            .field(CLUSTER_UUID_FIELD.getPreferredName(), getClusterUUID()).field(STATE_UUID_FIELD.getPreferredName(), getStateUUID());
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(indices, StreamOutput::writeString, (stream, uploadedMetadata) -> uploadedMetadata.writeTo(stream));
        out.writeVLong(term);
        out.writeVLong(version);
        out.writeString(clusterUUID);
        out.writeString(stateUUID);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClusterMetadataMarker that = (ClusterMetadataMarker) o;
        return Objects.equals(indices, that.indices)
            && term == that.term
            && version == that.version
            && Objects.equals(clusterUUID, that.clusterUUID)
            && Objects.equals(stateUUID, that.stateUUID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indices, term, version, clusterUUID, stateUUID);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    public static ClusterMetadataMarker fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Builder for ClusterMetadataMarker
     *
     * @opensearch.internal
     */
    public static class Builder {

        private final Map<String, UploadedIndexMetadata> indices;
        private long term;
        private long version;
        private String clusterUUID;
        private String stateUUID;

        public void term(long term) {
            this.term = term;
        }

        public void version(long version) {
            this.version = version;
        }

        public void clusterUUID(String clusterUUID) {
            this.clusterUUID = clusterUUID;
        }

        public void stateUUID(String stateUUID) {
            this.stateUUID = stateUUID;
        }

        public Map<String, UploadedIndexMetadata> getIndices() {
            return indices;
        }

        public Builder() {
            indices = new HashMap<>();
        }

        public ClusterMetadataMarker build() {
            return new ClusterMetadataMarker(indices, term, version, clusterUUID, stateUUID);
        }

    }

    /**
     * Metadata for uploaded index metadata
     *
     * @opensearch.internal
     */
    public static class UploadedIndexMetadata implements Writeable, ToXContentFragment {

        private static final ParseField INDEX_NAME_FIELD = new ParseField("index_name");
        private static final ParseField INDEX_UUID_FIELD = new ParseField("index_uuid");
        private static final ParseField UPLOADED_FILENAME_FIELD = new ParseField("uploaded_filename");

        private static String indexName(Object[] fields) {
            return (String) fields[0];
        }

        private static String indexUUID(Object[] fields) {
            return (String) fields[1];
        }

        private static String uploadedFilename(Object[] fields) {
            return (String) fields[2];
        }

        private static final ConstructingObjectParser<UploadedIndexMetadata, Void> PARSER = new ConstructingObjectParser<>(
            "uploaded_index_metadata",
            fields -> new UploadedIndexMetadata(indexName(fields), indexUUID(fields), uploadedFilename(fields))
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_NAME_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_UUID_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), UPLOADED_FILENAME_FIELD);
        }

        private final String indexName;
        private final String indexUUID;
        private final String uploadedFilename;

        public UploadedIndexMetadata(String indexName, String indexUUID, String uploadedFileName) {
            this.indexName = indexName;
            this.indexUUID = indexUUID;
            this.uploadedFilename = uploadedFileName;
        }

        public String getUploadedFilename() {
            return uploadedFilename;
        }

        public String getIndexName() {
            return indexName;
        }

        public String getIndexUUID() {
            return indexUUID;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject(getIndexName()).field(INDEX_NAME_FIELD.getPreferredName(), getIndexName())
                .field(INDEX_UUID_FIELD.getPreferredName(), getIndexUUID()).field(UPLOADED_FILENAME_FIELD.getPreferredName(), getUploadedFilename())
                .endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(indexName);
            out.writeString(indexUUID);
            out.writeString(uploadedFilename);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final UploadedIndexMetadata that = (UploadedIndexMetadata) o;
            return Objects.equals(indexName, that.indexName)
                && Objects.equals(indexUUID, that.indexUUID)
                && Objects.equals(uploadedFilename, that.uploadedFilename);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, indexUUID, uploadedFilename);
        }

        @Override
        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this);
        }

        public static UploadedIndexMetadata fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }
}
