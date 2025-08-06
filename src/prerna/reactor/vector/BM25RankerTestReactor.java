package prerna.reactor.vector;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class BM25RankerTestReactor extends AbstractReactor {

    public BM25RankerTestReactor() {
        this.keysToGet   = new String[] {};
        this.keyRequired = new int[]    {};
    }

    @Override
    public NounMetadata execute() {
        // Test 1 config
        String path1  = "C:\\workspace\\Semoss\\vector\\EmbeddingTests__2ec6a707-07f1-4e75-b4ff-5ea30629359b\\schema\\default\\indexed_files\\Official-Playing-Rules-2022-23-NBA-Season.csv";
        String query1 = "coach challenge rules";

        // Test 2 config
        Path parentDir = Paths.get(path1).getParent();
        String path2  = parentDir.resolve("TMPPM.csv").toString();
        String query2 = "Where to get Texas Medical provider information from";

        int topN = 3;
        Map<String, Object> ret = new HashMap<>();

        // Run both tests
        TestResult tr1 = runTest(path1, query1, topN);
        ret.put("TEST1_CORPUS_SIZE", tr1.corpusSize);
        ret.put("TEST1_QUERY",       tr1.query);
        ret.put("TEST1_RESULTS",     tr1.results);

        TestResult tr2 = runTest(path2, query2, topN);
        ret.put("TEST2_CORPUS_SIZE", tr2.corpusSize);
        ret.put("TEST2_QUERY",       tr2.query);
        ret.put("TEST2_RESULTS",     tr2.results);

        return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
    }

    /**
     * Executes a single BM25 test: loads the CSV column, invokes the reactor, and returns results.
     * Uses a unique index file derived from the CSV filename to avoid cache collision.
     */
    private TestResult runTest(String csvPath, String query, int topN) {
        // 1) Load column 6 (index 5) for corpus size
        List<String> corpus = loadColumn(csvPath, 5);

        // 2) Prepare and invoke the BM25RankerReactor
        BM25RankerReactor ranker = new BM25RankerReactor();
        ranker.keyValue = new HashMap<>();
        ranker.keyValue.put("CORPUS_PATH", csvPath);
        ranker.keyValue.put("QUERY",       query);
        ranker.keyValue.put("TOP_N",       String.valueOf(topN));

        // 3) Derive a unique index file name per CSV (basename_bm25_index.ser)
        Path csvFile = Paths.get(csvPath);
        String baseName = csvFile.getFileName().toString()
                            .replaceFirst("(?i)\\.csv$", "");
        String indexFile = csvFile.getParent()
                          .resolve(baseName + "_bm25_index.ser")
                          .toString();
        ranker.keyValue.put("INDEX_PATH_OVERRIDE", indexFile);

        NounMetadata nm = ranker.execute();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> results =
            (List<Map<String, Object>>)
            ((Map<String, Object>) nm.getValue())
            .get("BM25_RESULTS");

        return new TestResult(corpus.size(), query, results);
    }

    /**
     * Reads CSV at csvPath and extracts the specified column index into a list.
     */
    private List<String> loadColumn(String csvPath, int colIndex) {
        List<String> out = new ArrayList<>();
        String splitRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            br.readLine();  // skip header
            String line;
            while ((line = br.readLine()) != null) {
                String[] cols = line.split(splitRegex, -1);
                if (cols.length > colIndex) {
                    String cell = cols[colIndex].trim().replaceAll("^\"|\"$", "");
                    if (!cell.isEmpty()) {
                        out.add(cell);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load column from CSV: " + csvPath, e);
        }
        return out;
    }

    /** Simple holder for test outputs. */
    private static class TestResult {
        final int corpusSize;
        final String query;
        final List<Map<String, Object>> results;
        TestResult(int size, String q, List<Map<String, Object>> r) {
            this.corpusSize = size;
            this.query      = q;
            this.results    = r;
        }
    }
}
