package prerna.reactor.vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.store.ByteBuffersDirectory;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * BM25RankerService
 * 
 * Provides indexing, searching, and management of documents using Lucene's BM25 ranking.
 * Supports in-memory, disk-based, and S3-backed indexes.
 */
public class BM25RankerService implements Closeable {

    // Lucene directory for index storage
    private Directory luceneDirectory;
    // Analyzer for text processing
    private Analyzer analyzer;
    // Lucene IndexWriter for index modifications
    private IndexWriter writer;
    // Logger for class-level logging
    private static final Logger classLogger = LogManager.getLogger(AbstractVectorDatabaseEngine.class);
    // Path to disk-based index (null if in-memory)
    private String indexPath;

    // ------------------------------- Constructors -------------------------------

    /**
     * Constructor: Initializes an in-memory BM25 index.
     * 
     * @throws IOException if index initialization fails
     */
    public BM25RankerService() throws IOException {
        this.luceneDirectory = new ByteBuffersDirectory();
        this.analyzer = new StandardAnalyzer();
        this.writer = new IndexWriter(luceneDirectory, new IndexWriterConfig(analyzer));
        this.indexPath = null;
    }

    /**
     * Constructor: Initializes a disk-based BM25 index at the specified path.
     * 
     * @param indexPath Path to store the Lucene index
     * @throws IOException if index initialization fails
     */
    public BM25RankerService(String indexPath) throws IOException {
        classLogger.info("Initializing BM25RankerService with index path: " + indexPath);
        this.luceneDirectory = FSDirectory.open(Paths.get(indexPath));
        this.analyzer = new StandardAnalyzer();
        this.writer = new IndexWriter(luceneDirectory, new IndexWriterConfig(analyzer));
        this.indexPath = indexPath;
        classLogger.info("BM25RankerService initialized with disk-based index at: " + indexPath);   
    }

    /**
     * Static Factory: Loads a BM25 index from S3 by downloading to a temp directory.
     * 
     * @param bucket S3 bucket name
     * @param key S3 object key
     * @return BM25RankerService instance
     * @throws IOException if download or index initialization fails
     */
    public static BM25RankerService loadFromS3(String bucket, String key) throws IOException {
        // Implement your S3 download logic here
        Path tempDir = Files.createTempDirectory("lucene_index_s3");
        // ... download S3 object to tempDir ...
        BM25RankerService svc = new BM25RankerService(tempDir.toString());
        return svc;
    }

    // ------------------------------- Indexing Methods -------------------------------

    /**
     * Inserts new documents into the index, skipping duplicates by ID.
     * 
     * @param newDocs List of document contents
     * @param newIds List of document IDs (must match newDocs size)
     * @throws IOException if indexing fails
     * @throws IllegalArgumentException if input lists are mismatched
     */
    public void insertDocuments(List<String> newDocs, List<String> newIds) throws IOException {
        if (newDocs.size() != newIds.size()) throw new IllegalArgumentException("Docs and IDs must be same length.");
        Set<String> existingIds = new HashSet<>(getDocIds());
        int added = 0;
        for (int i = 0; i < newDocs.size(); i++) {
            String id = newIds.get(i);
            if (existingIds.contains(id)) continue;
            Document doc = new Document();
            doc.add(new StringField("id", id, Field.Store.YES));
            doc.add(new TextField("content", newDocs.get(i) == null ? "" : newDocs.get(i), Field.Store.YES));
            writer.addDocument(doc);
            added++;
        }
        writer.commit();
        String location = (indexPath == null) ? "in-memory" : indexPath;
        classLogger.info("BM25RankerService: Added " + added + " new document chunks (out of " + newDocs.size() + ") to index at: " + location);
    }

    /**
     * Removes multiple documents from the index by their IDs.
     *
     * @param docIds List of document IDs to remove
     * @throws IOException if deletion fails
     */
    public void removeDocumentsByIds(List<String> docIds) throws IOException {
        if (docIds == null || docIds.isEmpty()) {
            throw new IllegalArgumentException("docIds list must not be null or empty.");
        }
        int removed = 0;
        for (String id : docIds) {
            if (id == null) continue; // Skip null IDs
            Term idTerm = new Term("id", id);
            writer.deleteDocuments(idTerm);
            removed++;
        }
        writer.commit();
        String location = (indexPath == null) ? "in-memory" : indexPath;
        classLogger.info("BM25RankerService: Removed " + removed + " document chunks (out of " + docIds.size() + ") from index at: " + location);
    }

    /**
     * Deletes all documents and index files (if disk-based).
     * 
     * @throws IOException if deletion fails
     */
    public void deleteIndex() throws IOException {
        writer.deleteAll();
        writer.commit();
        classLogger.info("BM25RankerService: Deleted all document chunks from index.");
        // If disk-based, also delete index files from disk
        if (indexPath != null) {
            writer.close(); // Close writer before deleting files
            luceneDirectory.close(); // Close directory before deleting files
            Path indexDirPath = Paths.get(indexPath);
            try {
                Files.walk(indexDirPath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
                classLogger.info("BM25RankerService: Deleted index directory at: " + indexPath);
            } catch (IOException e) {
                classLogger.error("BM25RankerService: Failed to delete index directory at: " + indexPath, e);
                throw e;
            }
        }
    }

    // ------------------------------- Search Methods -------------------------------

    /**
     * Searches the index for documents matching the query using BM25 ranking.
     * 
     * @param query Query string
     * @param topN Maximum number of results to return
     * @return List of result maps (docId, score, content)
     * @throws Exception if search fails
     */
    public List<Map<String, Object>> search(String query, int topN) throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();
        try (DirectoryReader reader = DirectoryReader.open(luceneDirectory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            QueryParser parser = new QueryParser("content", analyzer);
            Query luceneQuery = parser.parse(query);
            TopDocs topDocs = searcher.search(luceneQuery, topN);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = searcher.doc(scoreDoc.doc);
                Map<String, Object> result = new HashMap<>();
                result.put("docId", doc.get("id"));
                result.put("score", scoreDoc.score);
                result.put("content", doc.get("content"));
                results.add(result);
            }
        } catch (Exception e) {
            System.out.println("[ERROR] Exception during search: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
        return results;
    }

    // ------------------------------- Utility Methods -------------------------------

    /**
     * Retrieves all document IDs currently in the index.
     * 
     * @return List of document IDs
     * @throws IOException if reading fails
     */
    public List<String> getDocIds() throws IOException {
        List<String> ids = new ArrayList<>();
        String[] files = luceneDirectory.listAll();
        boolean hasSegments = Arrays.stream(files).anyMatch(f -> f.startsWith("segments"));
        if (!hasSegments) return ids; // Return empty if index not initialized
        try (DirectoryReader reader = DirectoryReader.open(luceneDirectory)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                LeafReader leaf = ctx.reader();
                for (int i = 0; i < leaf.maxDoc(); i++) {
                    Document doc = leaf.document(i);
                    ids.add(doc.get("id"));
                }
            }
        }
        return ids;
    }

    /**
     * Retrieves the raw corpus (all document contents) from the index.
     * 
     * @return List of document contents
     * @throws IOException if reading fails
     */
    public List<String> getRawCorpus() throws IOException {
        List<String> corpus = new ArrayList<>();
        try (DirectoryReader reader = DirectoryReader.open(luceneDirectory)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                LeafReader leaf = ctx.reader();
                for (int i = 0; i < leaf.maxDoc(); i++) {
                    Document doc = leaf.document(i);
                    corpus.add(doc.get("content"));
                }
            }
        }
        return corpus;
    }

    /**
     * Closes the index writer and directory resources.
     * 
     * @throws IOException if closing fails
     */
    @Override
    public void close() throws IOException {
        writer.close();
        luceneDirectory.close();
    }

    // ------------------------------- Config-Based Loader -------------------------------

    /**
     * Loads a BM25RankerService instance from configuration properties.
     * 
     * @param props Configuration properties
     * @return BM25RankerService instance
     * @throws Exception if loading fails
     */
    public static BM25RankerService loadFromConfig(Properties props) throws Exception {
    	String methodProp = props.getProperty("BM25_INDEX_METHOD");
    	String method = (methodProp == null) ? "DISK" : methodProp.toUpperCase();
        switch (method) {
            case "DISK":
                String diskPath = props.getProperty("BM25_INDEX_PATH");
                System.out.println(diskPath + " : DiskPath");
                if (diskPath == null) throw new IllegalArgumentException("BM25_INDEX_PATH must be set for DISK method.");
                return new BM25RankerService(diskPath);
            case "MEMORY":
                return new BM25RankerService();
            case "S3":
                String bucket = props.getProperty("BM25_S3_BUCKET");
                String key = props.getProperty("BM25_S3_KEY");
                if (bucket == null || key == null) throw new IllegalArgumentException("BM25_S3_BUCKET and BM25_S3_KEY must be set for S3 method.");
                return loadFromS3(bucket, key);
            default:
                throw new IllegalArgumentException("Unknown BM25_INDEX_METHOD: " + method);
        }
    }
}
