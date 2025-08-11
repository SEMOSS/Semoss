package prerna.reactor.vector;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A BM25 service that indexes and ranks a provided corpus (list of documents) with composite IDs.
 */
public class BM25RankerService implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final double k1 = 1.5;
    private static final double b  = 0.75;

    // --- Index state ---
    private final List<List<String>> tokenizedCorpus = new ArrayList<>();
    private final List<String> rawCorpus             = new ArrayList<>();
    private final List<String> docIds                = new ArrayList<>(); // Composite IDs
    private final Map<String, Integer> docFreq       = new HashMap<>();
    private double avgDocLen = 0.0;

    public BM25RankerService() { }

    /**
     * Build a BM25 index from a corpus and composite IDs, then save it to disk.
     */
    public static BM25RankerService buildAndSave(List<String> corpus, List<String> ids, String indexFilePath) throws IOException {
        BM25RankerService svc = new BM25RankerService();
        svc.indexDocuments(corpus, ids);
        svc.saveIndex(indexFilePath);
        return svc;
    }

    /**
     * Load a BM25 index from a serialized file.
     */
    public static BM25RankerService loadFromIndex(String indexFilePath) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(indexFilePath))) {
            return (BM25RankerService) ois.readObject();
        }
    }

    /**
     * Save the current BM25 index to disk.
     */
    public void saveIndex(String indexFilePath) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(indexFilePath))) {
            oos.writeObject(this);
        }
    }

    /**
     * Incrementally insert new documents and IDs into the index.
     * Skips duplicate IDs.
     */
    public void insertDocuments(List<String> newDocs, List<String> newIds) {
        if (newDocs.size() != newIds.size()) {
            throw new IllegalArgumentException("New docs and IDs must be the same length.");
        }
        int added = 0;
        for (int i = 0; i < newDocs.size(); i++) {
            String doc = newDocs.get(i);
            String id  = newIds.get(i);
            if (docIds.contains(id)) {
                // Removed verbose print: System.out.println("Skipping duplicate ID: " + id);
                continue;
            }
            if (doc == null) doc = "";
            List<String> toks = tokenize(doc);
            tokenizedCorpus.add(toks);
            rawCorpus.add(doc);
            docIds.add(id);

            // Update docFreq for unique tokens in this document
            Set<String> uniqueTokens = new HashSet<>(toks);
            for (String token : uniqueTokens) {
                docFreq.put(token, docFreq.getOrDefault(token, 0) + 1);
            }
            added++;
        }
        // Update average document length
        int totalDocLen = tokenizedCorpus.stream().mapToInt(List::size).sum();
        avgDocLen = docIds.isEmpty() ? 0.0 : ((double) totalDocLen) / docIds.size();
        // Removed verbose print: System.out.println("Inserted " + added + " new documents. Corpus size is now " + docIds.size());
    }

    /**
     * Search the in-memory BM25 index.
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
                double termScore = idf * (freq * (k1 + 1)) / denom;
                score += termScore;
            }
            Map<String,Object> result = new HashMap<>();
            result.put("docId",  docIds.get(i)); // Use composite ID
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

    /** Exposes the raw corpus. */
    public List<String> getRawCorpus() {
        return Collections.unmodifiableList(rawCorpus);
    }

    /** Exposes the doc IDs. */
    public List<String> getDocIds() {
        return Collections.unmodifiableList(docIds);
    }
    
    public static BM25RankerService loadFromConfig(Properties smssProps) throws IOException, ClassNotFoundException {
        String method = smssProps.getProperty("BM25_INDEX_METHOD").toUpperCase();
        switch (method) {
            case "DISK":
                String diskPath = smssProps.getProperty("BM25_INDEX_PATH");
                if (diskPath == null) throw new IllegalArgumentException("BM25_INDEX_PATH must be set for DISK method.");
                return loadFromIndex(diskPath);
            case "MEMORY":
                // For MEMORY, you must build the index at runtime. Example:
                // You'd need to provide corpus/ids in your service call or as a path in .smss
                throw new UnsupportedOperationException("MEMORY method requires corpus/ids at runtime.");
            case "S3":
                String bucket = smssProps.getProperty("BM25_S3_BUCKET");
                String key = smssProps.getProperty("BM25_S3_KEY");
                if (bucket == null || key == null) throw new IllegalArgumentException("BM25_S3_BUCKET and BM25_S3_KEY must be set for S3 method.");
                return loadFromS3(bucket, key);
            default:
                throw new IllegalArgumentException("Unknown BM25_INDEX_METHOD: " + method);
        }
    }

    // Example S3 loader (requires AWS SDK)
    public static BM25RankerService loadFromS3(String bucket, String key) throws IOException, ClassNotFoundException {
        // Use AWS SDK to download the file to a temp location or stream it directly
        // Example using temp file:
        File tempFile = File.createTempFile("bm25_index", ".ser");
        // ... download S3 object to tempFile ...
        // (You must implement the S3 download logic using your AWS SDK of choice)
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tempFile))) {
            return (BM25RankerService) ois.readObject();
        } finally {
            tempFile.delete();
        }
    }

    // ----- Internal helpers -----

    /** Tokenizes text on non-word characters. */
    private List<String> tokenize(String text) {
        if (text == null) return Collections.emptyList();
        return Arrays.stream(text.toLowerCase().split("\\W+"))
                     .filter(tok -> !tok.isEmpty())
                     .collect(Collectors.toList());
    }

    /** Builds the in-memory BM25 index from raw docs and IDs. */
    void indexDocuments(List<String> documents, List<String> ids) {
        if (documents.size() != ids.size()) {
            throw new IllegalArgumentException("Corpus and IDs must be the same length.");
        }

        tokenizedCorpus.clear();
        rawCorpus.clear();
        docIds.clear();
        docFreq.clear();
        avgDocLen = 0.0;

        int totalDocLen = 0;

        for (int i = 0; i < documents.size(); i++) {
            String doc = documents.get(i);
            String id  = ids.get(i);
            if (doc == null) {
                // Removed verbose print: System.err.println("Warning: Document at index " + i + " with ID '" + id + "' is null.");
                doc = "";
            }
            List<String> toks = tokenize(doc);
            tokenizedCorpus.add(toks);
            rawCorpus.add(doc);
            docIds.add(id);

            // Update docFreq for unique tokens in this document
            Set<String> uniqueTokens = new HashSet<>(toks);
            for (String token : uniqueTokens) {
                docFreq.put(token, docFreq.getOrDefault(token, 0) + 1);
            }
            totalDocLen += toks.size();
        }
        avgDocLen = documents.isEmpty() ? 0.0 : ((double) totalDocLen) / documents.size();
    }
}
