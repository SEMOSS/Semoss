package prerna.reactor.vector;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;

import java.util.*;

public class BM25RankerIndexDeleteReactor extends AbstractReactor {

    private static final String INDEX_OVERRIDE_KEY = "INDEX_PATH_OVERRIDE";
    private static final String INDEX_METHOD_KEY = "INDEX_METHOD_KEY";

    public BM25RankerIndexDeleteReactor() {
        this.keysToGet = new String[] { INDEX_OVERRIDE_KEY, INDEX_METHOD_KEY };
        this.keyRequired = new int[] { 0, 0 };
    }

    @Override
    public NounMetadata execute() {
        String indexFp = getLiteralFromNounStore(INDEX_OVERRIDE_KEY);
        String indexMethod = getLiteralFromNounStore(INDEX_METHOD_KEY);
        if (indexFp == null || indexFp.isEmpty()) indexFp = "bm25_index_dir";
        if (indexMethod == null) indexMethod = "DISK";

        BM25RankerService service = null;
        Map<String,Object> payload = new HashMap<>();
        try {
            service = "DISK".equalsIgnoreCase(indexMethod) ? new BM25RankerService(indexFp) : new BM25RankerService();
            service.deleteIndex();
            payload.put("BM25_INDEX_DELETED", true);
        } catch (Exception e) {
            throw new RuntimeException("Error in BM25RankerIndexDeleteReactor", e);
        } finally {
            if (service != null) try { service.close(); } catch (Exception ignore) {}
        }
        return new NounMetadata(payload, PixelDataType.CUSTOM_DATA_STRUCTURE);
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
        return "Deletes the entire BM25 vector index. Useful for resetting or removing all indexed data.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (INDEX_OVERRIDE_KEY.equals(key)) {
            return "Optional: File path to override the default BM25 index directory.";
        } else if (INDEX_METHOD_KEY.equals(key)) {
            return "Optional: Method for accessing the index (e.g., 'DISK' for disk-based index).";
        }
        return super.getDescriptionForKey(key);
    }
}
