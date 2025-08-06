package prerna.reactor.vector;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.PixelDataType;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BM25RankerReactor extends AbstractReactor {

    private static final int CONTENT_COLUMN_INDEX = 5; // zero-based index of the 6th CSV column
    private static final String INDEX_FILENAME     = "bm25_index.ser";
    private static final String INDEX_OVERRIDE_KEY = "INDEX_PATH_OVERRIDE";

    public BM25RankerReactor() {
        this.keysToGet   = new String[] { "CORPUS_PATH", "QUERY", "TOP_N", INDEX_OVERRIDE_KEY };
        this.keyRequired = new int[]    { 1,              1,       0,       0              };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String csvPath    = keyValue.get("CORPUS_PATH");
        String query      = keyValue.get("QUERY");
        int topN          = parseTopN(keyValue.get("TOP_N"), 3);
        String overrideFp = keyValue.get(INDEX_OVERRIDE_KEY);

        // Derive default index file alongside the CSV
        Path csv    = Paths.get(csvPath);
        Path parent = csv.getParent() != null ? csv.getParent() : Paths.get(".");
        String defaultIndexFp = parent.resolve(INDEX_FILENAME).toString();

        // If override provided, use it; otherwise use default
        String indexFp = (overrideFp != null && !overrideFp.isEmpty())
                         ? overrideFp
                         : defaultIndexFp;

        // Load or build the BM25 service from CSV (6th column extraction)
        BM25RankerService service = initService(csvPath, CONTENT_COLUMN_INDEX, indexFp);

        // Optional: log extracted column for debugging
        logExtractedContent(service.getRawCorpus());

        // Execute the search
        List<Map<String,Object>> results = service.search(query, topN);

        // Wrap and return
        return wrapResults(results);
    }

    /** Safely parse TOP_N or fallback to default. */
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

    /** Initialize BM25RankerService via CSV loading or existing index. */
    private BM25RankerService initService(String csvPath, int colIndex, String indexFile) {
        try {
            return BM25RankerService.loadOrBuildFromCsv(csvPath, colIndex, indexFile);
        } catch (Exception e) {
            throw new RuntimeException(
                "Error initializing BM25RankerService from CSV: " + csvPath, e);
        }
    }

    /** Print the extracted 6th-column contents for verification. */
    private void logExtractedContent(List<String> extracted) {
        System.out.println("=== Extracted Content Column (size=" + extracted.size() + ") ===");
        for (int i = 0; i < extracted.size(); i++) {
            System.out.println("[" + i + "] " + extracted.get(i));
        }
        System.out.println("=== End Extracted Content ===");
    }

    /** Wrap BM25 results into SableCC noun metadata. */
    private NounMetadata wrapResults(List<Map<String,Object>> results) {
        Map<String,Object> payload = new HashMap<>();
        payload.put("BM25_RESULTS", results);
        return new NounMetadata(payload, PixelDataType.CUSTOM_DATA_STRUCTURE);
    }
}
