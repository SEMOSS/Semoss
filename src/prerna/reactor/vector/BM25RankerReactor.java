package prerna.reactor.vector;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;

import java.io.File;
import java.util.*;

public class BM25RankerReactor extends AbstractReactor {

    private static final String INDEX_FILENAME     = "bm25_index.ser";
    private static final String INDEX_OVERRIDE_KEY = "INDEX_PATH_OVERRIDE";
    private static final String ACTION_KEY         = "BM25_ACTION"; // "insert" or "search"
    private static final String INDEX_METHOD_KEY   = "INDEX_METHOD_KEY"; // Options: DISK, MEMORY, S3
    private static final String S3_BUCKET_KEY      = "S3_BUCKET_KEY";
    private static final String S3_KEY_KEY         = "S3_KEY_KEY";

    public BM25RankerReactor() {
        this.keysToGet   = new String[] {
            ReactorKeysEnum.CORPUS_KEY.getKey(),
            ReactorKeysEnum.CORPUS_IDS_KEY.getKey(),
            "QUERY", "TOP_N", INDEX_OVERRIDE_KEY, ACTION_KEY, INDEX_METHOD_KEY
        };
        this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0 };
    }

    @Override
    public NounMetadata execute() {

        List<String> corpus = getAsStringListFromNounStore(ReactorKeysEnum.CORPUS_KEY.getKey());
        List<String> ids    = getAsStringListFromNounStore(ReactorKeysEnum.CORPUS_IDS_KEY.getKey());
        String query        = getLiteralFromNounStore("QUERY");
        int topN            = parseTopN(getLiteralFromNounStore("TOP_N"), 3);
        String indexFp      = getLiteralFromNounStore(INDEX_OVERRIDE_KEY);
        String action       = getLiteralFromNounStore(ACTION_KEY);

        if (indexFp == null || indexFp.isEmpty()) {
            indexFp = INDEX_FILENAME;
        }
        File idxFile = new File(indexFp);

        // Default to "search" if action is missing or invalid
        boolean isInsert = "insert".equalsIgnoreCase(action);

        String indexMethod = getLiteralFromNounStore(INDEX_METHOD_KEY);
        if (indexMethod == null) indexMethod = "DISK"; // default

        BM25RankerService service = null;
        try {
            switch (indexMethod.toUpperCase()) {
                case "DISK":
                    if (idxFile.exists()) {
                        service = BM25RankerService.loadFromIndex(indexFp);
                    } else {
                        service = BM25RankerService.buildAndSave(corpus, ids, indexFp);
                    }
                    break;
                case "S3":
                    String bucket = getLiteralFromNounStore(S3_BUCKET_KEY);
                    String s3key  = getLiteralFromNounStore(S3_KEY_KEY);
                    if (bucket == null || s3key == null)
                        throw new IllegalArgumentException("BM25_S3_BUCKET and BM25_S3_KEY must be provided for S3 method.");
                    service = BM25RankerService.loadFromS3(bucket, s3key);
                    break;
                case "MEMORY":
                    // Always build index in memory from corpus/ids
                    service = new BM25RankerService();
                    service.indexDocuments(corpus, ids);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown BM25_INDEX_METHOD: " + indexMethod);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error initializing BM25RankerService", e);
        }
        
        Map<String,Object> payload = new HashMap<>();

        if (isInsert) {
            // Insert mode: add new documents and IDs, then save
            service.insertDocuments(corpus, ids);
            try {
                service.saveIndex(indexFp);
            } catch (Exception e) {
                throw new RuntimeException("Failed to save BM25 index after insert", e);
            }
            payload.put("BM25_INSERTED", corpus.size());
            payload.put("BM25_INDEX_FILE", indexFp);
            payload.put("BM25_DOC_COUNT", service.getDocIds().size());
        } else {
            // Search mode: run query and return results
            List<Map<String,Object>> results = service.search(query, topN);
            payload.put("BM25_RESULTS", results);
            payload.put("BM25_INDEX_FILE", indexFp);
        }

        // Single diagnostic print at the end
        System.out.println("BM25RankerReactor payload: " + payload);

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
    private List<String> getAsStringListFromNounStore(String key) {
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
    private String getLiteralFromNounStore(String key) {
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
}
