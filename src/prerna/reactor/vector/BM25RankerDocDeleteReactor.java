package prerna.reactor.vector;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;

import java.util.*;

public class BM25RankerDocDeleteReactor extends AbstractReactor {

    private static final String DOC_IDS_KEY = "DOC_IDS";
    private static final String INDEX_OVERRIDE_KEY = "INDEX_PATH_OVERRIDE";
    private static final String INDEX_METHOD_KEY = "INDEX_METHOD_KEY";

    public BM25RankerDocDeleteReactor() {
        this.keysToGet = new String[] { DOC_IDS_KEY, INDEX_OVERRIDE_KEY, INDEX_METHOD_KEY };
        this.keyRequired = new int[] { 1, 0, 0 }; // DOC_IDS required
    }

    @Override
    public NounMetadata execute() {
        List<String> docIds = getAsStringListFromNounStore(DOC_IDS_KEY);
        if (docIds == null || docIds.isEmpty()) {
            return NounMetadata.getErrorNounMessage("No document IDs provided for deletion.");
        }
        String indexFp = getLiteralFromNounStore(INDEX_OVERRIDE_KEY);
        String indexMethod = getLiteralFromNounStore(INDEX_METHOD_KEY);
        if (indexFp == null || indexFp.isEmpty()) indexFp = "bm25_index_dir";
        if (indexMethod == null) indexMethod = "DISK";

        BM25RankerService service = null;
        Map<String,Object> payload = new HashMap<>();
        try {
            service = "DISK".equalsIgnoreCase(indexMethod) ? new BM25RankerService(indexFp) : new BM25RankerService();
            service.removeDocumentsByIds(docIds);
            payload.put("BM25_DELETED_IDS", docIds);
        } catch (Exception e) {
            throw new RuntimeException("Error in BM25RankerDocDeleteReactor", e);
        } finally {
            if (service != null) try { service.close(); } catch (Exception ignore) {}
        }
        return new NounMetadata(payload, PixelDataType.CUSTOM_DATA_STRUCTURE);
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
        return "Deletes specified documents from the BM25 vector index using their document IDs. Supports optional index path and method overrides.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (DOC_IDS_KEY.equals(key)) {
            return "Required: List of document IDs to delete from the BM25 index.";
        } else if (INDEX_OVERRIDE_KEY.equals(key)) {
            return "Optional: File path to override the default BM25 index directory.";
        } else if (INDEX_METHOD_KEY.equals(key)) {
            return "Optional: Method for accessing the index (e.g., 'DISK' for disk-based index).";
        }
        return super.getDescriptionForKey(key);
    }
}
