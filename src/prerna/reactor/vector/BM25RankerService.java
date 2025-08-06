package prerna.reactor.vector;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A BM25 service that solely loads and indexes the 6th column from a CSV file.
 */
public class BM25RankerService implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final double k1 = 1.5;
    private static final double b  = 0.75;

    // --- Index state ---
    private final List<List<String>> tokenizedCorpus = new ArrayList<>();
    private final List<String> rawCorpus              = new ArrayList<>();
    private final Map<String, Integer> docFreq        = new HashMap<>();
    private double avgDocLen = 0.0;

    /** No-arg constructor */
    public BM25RankerService() { }

    /**
     * Load or build a BM25 index directly from the CSV.
     *
     * @param csvPath          Path to the CSV file.
     * @param contentColIndex  Zero-based index of the desired content column.
     * @param indexFilePath    Where to read/write the serialized index.
     */
    public static BM25RankerService loadOrBuildFromCsv(
            String csvPath, int contentColIndex, String indexFilePath)
            throws IOException, ClassNotFoundException {

        File idxFile = new File(indexFilePath);
        if (idxFile.exists()) {
            // Load existing index
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(idxFile))) {
                return (BM25RankerService) ois.readObject();
            }
        } else {
            // Build new index from CSV
            BM25RankerService svc = new BM25RankerService();
            svc.indexCsvAndSave(csvPath, contentColIndex, indexFilePath);
            return svc;
        }
    }

    /**
     * Indexes the specified CSV column and saves the built index to disk.
     *
     * @param csvPath          Path to CSV.
     * @param contentColIndex  Zero-based column index to extract.
     * @param indexFilePath    Where to write the serialized service.
     */
    public void indexCsvAndSave(String csvPath, int contentColIndex, String indexFilePath)
            throws IOException {

        List<String> docs = loadColumnFromCsv(csvPath, contentColIndex);
        indexDocuments(docs);

        // Persist the service state
        try (ObjectOutputStream oos = new ObjectOutputStream(
                 new FileOutputStream(indexFilePath))) {
            oos.writeObject(this);
        }
    }

    /**
     * Search the in-memory BM25 index.
     *
     * @param query The query string.
     * @param topN  Max number of results to return.
     * @return List of maps with keys "docId", "score", and "content".
     */
    public List<Map<String, Object>> search(String query, int topN) {
        int N = tokenizedCorpus.size();
        List<String> qTokens = tokenize(query);

        List<Map<String,Object>> scored = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            List<String> dTokens = tokenizedCorpus.get(i);
            double score = 0.0;
            for (String term : qTokens) {
                int freq = Collections.frequency(dTokens, term);
                if (freq == 0) continue;
                int df = docFreq.getOrDefault(term, 0);
                double idf = Math.log(1 + (N - df + 0.5) / (df + 0.5));
                double denom = freq + k1 * (1 - b + b * dTokens.size() / avgDocLen);
                score += idf * (freq * (k1 + 1)) / denom;
            }
            Map<String,Object> result = new HashMap<>();
            result.put("docId",  i);
            result.put("score",  score);
            result.put("content", rawCorpus.get(i));
            scored.add(result);
        }

        return scored.stream()
                     .sorted((a, b) -> Double.compare((Double)b.get("score"),
                                                      (Double)a.get("score")))
                     .limit(topN)
                     .collect(Collectors.toList());
    }

    /** Exposes the raw extracted CSV column. */
    public List<String> getRawCorpus() {
        return Collections.unmodifiableList(rawCorpus);
    }

    // ----- Internal helpers -----

    /** Tokenizes text on non-word characters. */
    private List<String> tokenize(String text) {
        return Arrays.stream(text.toLowerCase().split("\\W+"))
                     .filter(tok -> !tok.isEmpty())
                     .collect(Collectors.toList());
    }

    /** Builds the in-memory BM25 index from raw docs. */
    private void indexDocuments(List<String> documents) {
        tokenizedCorpus.clear();
        rawCorpus.clear();
        docFreq.clear();
        avgDocLen = 0.0;

        for (String doc : documents) {
            List<String> toks = tokenize(doc);
            tokenizedCorpus.add(toks);
            rawCorpus.add(doc);
        }
        for (List<String> toks : tokenizedCorpus) {
            Set<String> uniq = new HashSet<>(toks);
            for (String term : uniq) {
                docFreq.put(term, docFreq.getOrDefault(term, 0) + 1);
            }
        }
        avgDocLen = tokenizedCorpus.stream()
                .mapToInt(List::size)
                .average()
                .orElse(0.0);
    }

    /**
     * Reads a CSV and extracts one column (handles quoted commas).
     */
    private List<String> loadColumnFromCsv(String csvPath, int colIndex) throws IOException {
        List<String> out = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(Paths.get(csvPath))) {
            String header = br.readLine();  // skip header
            String line;
            while ((line = br.readLine()) != null) {
                List<String> cols = parseCsvLine(line);
                if (cols.size() > colIndex) {
                    String cell = cols.get(colIndex).trim();
                    // remove wrapping quotes if present
                    if (cell.startsWith("\"") && cell.endsWith("\"")) {
                        cell = cell.substring(1, cell.length()-1);
                    }
                    out.add(cell);
                }
            }
        }
        return out;
    }

    /**
     * Splits a CSV line into fields, respecting quotes and escaped quotes.
     */
    private List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"' ) {
                if (inQuotes && i+1 < line.length() && line.charAt(i+1) == '"') {
                    sb.append('"'); 
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                fields.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        fields.add(sb.toString());
        return fields;
    }
}
