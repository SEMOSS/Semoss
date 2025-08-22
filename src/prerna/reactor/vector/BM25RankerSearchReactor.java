package prerna.reactor.vector;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;

import java.io.File;
import java.util.*;

public class BM25RankerSearchReactor extends AbstractReactor {

    private static final String INDEX_FILENAME     = "bm25_index_dir";
    private static final String INDEX_OVERRIDE_KEY = "INDEX_PATH_OVERRIDE";
    private static final String INDEX_METHOD_KEY   = "INDEX_METHOD_KEY";
    private static final String S3_BUCKET_KEY      = "S3_BUCKET_KEY";
    private static final String S3_KEY_KEY         = "S3_KEY_KEY";

    public BM25RankerSearchReactor() {
        this.keysToGet   = new String[] {
            "QUERY", "TOP_N", INDEX_OVERRIDE_KEY, INDEX_METHOD_KEY
        };
        this.keyRequired = new int[] { 1, 0, 0, 0 };
    }

    @Override
    public NounMetadata execute() {
        String query        = getLiteralFromNounStore("QUERY");
        int topN            = parseTopN(getLiteralFromNounStore("TOP_N"), 3);
        String indexFp      = getLiteralFromNounStore(INDEX_OVERRIDE_KEY);
        String indexMethod  = getLiteralFromNounStore(INDEX_METHOD_KEY);
        if (indexFp == null || indexFp.isEmpty()) indexFp = INDEX_FILENAME;
        if (indexMethod == null) indexMethod = "DISK";

        BM25RankerService service = null;
        Map<String,Object> payload = new HashMap<>();

        try {
            switch (indexMethod.toUpperCase()) {
                case "DISK":
                    service = new BM25RankerService(indexFp);
                    break;
                case "S3":
                    String bucket = getLiteralFromNounStore(S3_BUCKET_KEY);
                    String s3key  = getLiteralFromNounStore(S3_KEY_KEY);
                    if (bucket == null || s3key == null)
                        throw new IllegalArgumentException("BM25_S3_BUCKET and BM25_S3_KEY must be provided for S3 method.");
                    service = BM25RankerService.loadFromS3(bucket, s3key);
                    break;
                case "MEMORY":
                    service = new BM25RankerService();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown BM25_INDEX_METHOD: " + indexMethod);
            }
            List<Map<String,Object>> results = service.search(query, topN);
            payload.put("BM25_RESULTS", results);
            payload.put("BM25_INDEX_FILE", indexFp);
        } catch (Exception e) {
            throw new RuntimeException("Error in BM25SearchReactor", e);
        } finally {
            if (service != null) try { service.close(); } catch (Exception ignore) {}
        }
        return new NounMetadata(payload, PixelDataType.CUSTOM_DATA_STRUCTURE);
    }
    
    private int parseTopN(String str, int defaultN) {
        if (str == null || str.isEmpty()) {
            return defaultN;
        }
        try {
            int val = Integer.parseInt(str);
            return val > 0 ? val : defaultN;
        } catch (NumberFormatException e) {
            return defaultN;
        }
    }

    /**
     * Extracts a List<String> from a GenRowStruct in the NounStore.
     */
    public List<String> getAsStringListFromNounStore(String key) {
        Object noun = this.getNounStore().getNoun(key);
        List<String> result = new ArrayList<>();
        if (noun instanceof GenRowStruct) {
            GenRowStruct grs = (GenRowStruct) noun;
            for (int i = 0; i < grs.size(); i++) {
                Object val = grs.get(i);
                if (val != null) result.add(val.toString());
            }
        } else if (noun != null) {
            result.add(noun.toString());
        }
        return result;
    }

    /**
     * Extracts a single literal value (String) from a NounStore key.
     */
    public String getLiteralFromNounStore(String key) {
        Object noun = this.getNounStore().getNoun(key);
        if (noun instanceof GenRowStruct) {
            GenRowStruct grs = (GenRowStruct) noun;
            if (!grs.isEmpty()) {
                Object val = grs.get(0);
                return val == null ? null : val.toString();
            }
        } else if (noun != null) {
            return noun.toString();
        }
        return null;
    }
    
    @Override
    public String getReactorDescription() {
        StringBuilder headerBuilder = new StringBuilder();
        headerBuilder.append("'Query', ")
            .append("'Top N', ")
            .append("'Document ID', ")
            .append("'Score', ")
            .append("'Content', ")
            .append("'Index Path', ")
            .append("'Index Method'");

        return "Search for documents in the BM25 vector index using a query string. "
            + "Returns the top matching documents with the following fields: "
            + headerBuilder.toString();
    }
}
