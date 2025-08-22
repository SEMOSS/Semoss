package prerna.reactor.vector;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;

import java.io.File;
import java.util.*;

public class BM25RankerDeleteReactor extends AbstractReactor {

    private static final String INDEX_FILENAME     = "bm25_index_dir";
    private static final String INDEX_OVERRIDE_KEY = "INDEX_PATH_OVERRIDE";
    private static final String INDEX_METHOD_KEY   = "INDEX_METHOD_KEY";
    private static final String DOC_IDS_KEY        = "DOC_IDS"; // List of IDs to delete

    public BM25RankerDeleteReactor() {
        this.keysToGet   = new String[] { DOC_IDS_KEY, INDEX_OVERRIDE_KEY, INDEX_METHOD_KEY };
        this.keyRequired = new int[] { 0, 0, 0 }; // DOC_IDS_KEY optional: if missing, delete all
    }

    @Override
    public NounMetadata execute() {
        List<String> docIds = getAsStringListFromNounStore(DOC_IDS_KEY);
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
                case "MEMORY":
                    service = new BM25RankerService();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown BM25_INDEX_METHOD: " + indexMethod);
            }
            if (docIds != null && !docIds.isEmpty()) {
                service.removeDocumentsByIds(docIds);
                payload.put("BM25_DELETED_IDS", docIds);
            } else {
                service.deleteIndex();
                payload.put("BM25_INDEX_DELETED", true);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error in BM25DeleteReactor", e);
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
        StringBuilder headerBuilder = new StringBuilder();
        headerBuilder.append("'Document ID', ")
            .append("'Index Path', ")
            .append("'Index Method', ")
            .append("'Delete Status'");

        return "Delete documents from the BM25 vector index by their IDs, or remove the entire index if no IDs are provided. "
            + "The operation returns the following fields: "
            + headerBuilder.toString();
    }
}
