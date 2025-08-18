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
import java.util.stream.Collectors;

public class BM25RankerService implements Closeable {
    private Directory luceneDirectory;
    private Analyzer analyzer;
    private IndexWriter writer;
    private static final Logger classLogger = LogManager.getLogger(AbstractVectorDatabaseEngine.class);
    private String indexPath; // For disk-based index

    // --- Constructors ---

    // In-memory index
    public BM25RankerService() throws IOException {
    	this.luceneDirectory = new ByteBuffersDirectory();
        this.analyzer = new StandardAnalyzer();
        this.writer = new IndexWriter(luceneDirectory, new IndexWriterConfig(analyzer));
        this.indexPath = null;
    }

    public BM25RankerService(String indexPath) throws IOException {
        classLogger.info("Initializing BM25RankerService with index path: " + indexPath);
        this.luceneDirectory = FSDirectory.open(Paths.get(indexPath));
        this.analyzer = new StandardAnalyzer();
        this.writer = new IndexWriter(luceneDirectory, new IndexWriterConfig(analyzer));
        this.indexPath = indexPath;
        classLogger.info("BM25RankerService initialized with disk-based index at: " + indexPath);   
    }
    
    
    // S3-based index (download to temp dir, then open)
    public static BM25RankerService loadFromS3(String bucket, String key) throws IOException {
        // Implement your S3 download logic here
        // For example, download and unzip index to a temp directory:
        Path tempDir = Files.createTempDirectory("lucene_index_s3");
        // ... download S3 object to tempDir ...
        // (You must implement the S3 download logic using your AWS SDK of choice)
        BM25RankerService svc = new BM25RankerService(tempDir.toString());
        return svc;
    }

    // --- Indexing ---

    public void indexDocuments(List<String> docs, List<String> ids) throws IOException {
        if (docs.size() != ids.size()) throw new IllegalArgumentException("Docs and IDs must be same length.");
        for (int i = 0; i < docs.size(); i++) {
            Document doc = new Document();
            doc.add(new StringField("id", ids.get(i), Field.Store.YES));
            doc.add(new TextField("content", docs.get(i) == null ? "" : docs.get(i), Field.Store.YES));
            writer.addDocument(doc);
        }
        writer.commit();
    }

    // Insert new documents (skips duplicate IDs)
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
        classLogger.info("BM25RankerService: Added " + added + " new documents (out of " + newDocs.size() + ") to index at: " + location);
    }

    // --- Search ---

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
        }
        return results;
    }

    // --- Utility methods ---

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

    // --- Save index to disk (for disk or S3 upload) ---

    public void saveIndex(String path) throws IOException {
    	classLogger.info("Lucene index is saved on disk at: " + path);
        // For disk, Lucene index is already on disk at 'path'
        // For S3, upload all files in the index directory
        if (luceneDirectory instanceof FSDirectory) {
            // Implement S3 upload logic here if needed
            // Example: upload all files in 'path' directory to S3
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
        luceneDirectory.close();
    }

    // --- Config-based loader ---

    public static BM25RankerService loadFromConfig(Properties props) throws Exception {
        String method = props.getProperty("BM25_INDEX_METHOD").toUpperCase();
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
