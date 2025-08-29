package prerna.engine.impl.vector;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Utility functions for fusing and ranking search results from BM25 and semantic vector models.
 */
public class VectorRankingUtils {

    /**
     * Reciprocal Rank Fusion (RRF) combines two ranked lists (BM25 and vector results)
     * into a single ranked list using the RRF formula.
     *
     * @param bm25Results   List of BM25 search result maps (each must have "content")
     * @param vectorResults List of vector search result maps (each must have "content")
     * @param topN          Number of top results to return
     * @return Fused and ranked list of result maps, each with an added "rrf_score"
     */
    public static List<Map<String, Object>> rrfFuse(
            List<Map<String, Object>> bm25Results,
            List<Map<String, Object>> vectorResults,
            int topN) {

        // Map to accumulate RRF scores for each unique content
        Map<String, Double> rrfScores = new HashMap<>();
        int k = 30; // RRF constant (controls score decay)

        // Accumulate RRF scores for each result list
        BiConsumer<List<Map<String, Object>>, Integer> accumulate = (results, offset) -> {
            for (int i = 0; i < results.size(); i++) {
                String content = (String) results.get(i).get("content");
                if (content == null) continue;
                // RRF score: 1 / (k + rank)
                double score = 1.0 / (k + i + offset);
                double prevScore = rrfScores.getOrDefault(content, 0.0);
                rrfScores.put(content, prevScore + score);
            }
        };

        // Apply RRF scoring to both BM25 and vector results
        accumulate.accept(bm25Results, 0);
        accumulate.accept(vectorResults, 0);

        // Build a map from content to its result map (preserving all fields)
        Map<String, Map<String, Object>> contentMap = new HashMap<>();
        for (Map<String, Object> result : bm25Results) {
            String content = (String) result.get("content");
            if (content != null) contentMap.put(content, new HashMap<>(result));
        }
        for (Map<String, Object> result : vectorResults) {
            String content = (String) result.get("content");
            if (content != null && !contentMap.containsKey(content)) {
                contentMap.put(content, new HashMap<>(result));
            }
        }

        // Sort contents by RRF score descending
        List<Map.Entry<String, Double>> sorted = rrfScores.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .collect(Collectors.toList());

        // Build fused result list with top N entries, adding "rrf_score"
        List<Map<String, Object>> fused = new ArrayList<>();
        for (int i = 0; i < Math.min(topN, sorted.size()); i++) {
            Map.Entry<String, Double> entry = sorted.get(i);
            Map<String, Object> doc = contentMap.get(entry.getKey());
            doc.put("rrf_score", entry.getValue());
            fused.add(doc);
        }

        return fused;
    }

    /**
     * Hybrid fusion combines BM25 and vector results using weighted normalized scores.
     * Each result's score is normalized and then combined: final_score = alpha * bm25_norm + beta * vector_norm
     *
     * @param bm25Results   List of BM25 result maps (must have "content" and "score")
     * @param vectorResults List of vector result maps (must have "Content" and "Score")
     * @param topN          Number of top results to return
     * @param alpha         Weight for BM25 normalized score
     * @param beta          Weight for vector normalized score
     * @return Fused and ranked list of result maps, each with an added "hybrid_score"
     */
    public static List<Map<String, Object>> hybridFuse(
            List<Map<String, Object>> bm25Results,
            List<Map<String, Object>> vectorResults,
            int topN,
            double alpha,  // weight for BM25 score
            double beta    // weight for semantic score
    ) {

        // Collect all unique contents from both result sets
        Set<String> allContents = new HashSet<>();
        for (Map<String, Object> r : bm25Results) allContents.add((String) r.get("content"));
        for (Map<String, Object> r : vectorResults) {
            Object contentObj = r.get("Content");
            if (contentObj != null) allContents.add(contentObj.toString());
        }

        // Build BM25 rank and score maps, and track min/max for normalization
        Map<String, Integer> bm25Ranks = new HashMap<>();
        Map<String, Float> bm25Scores = new HashMap<>();
        float bm25Min = Float.MAX_VALUE, bm25Max = Float.MIN_VALUE;
        for (int i = 0; i < bm25Results.size(); i++) {
            String content = (String) bm25Results.get(i).get("content");
            bm25Ranks.put(content, i);
            Object scoreObj = bm25Results.get(i).get("score");
            float score = 0.0f;
            if (scoreObj instanceof Number) {
                score = ((Number) scoreObj).floatValue();
                bm25Scores.put(content, score);
                bm25Min = Math.min(bm25Min, score);
                bm25Max = Math.max(bm25Max, score);
            } else {
                bm25Scores.put(content, score);
            }
        }

        // Build vector rank and score maps, and track min/max for normalization
        Map<String, Integer> vectorRanks = new HashMap<>();
        Map<String, Float> vectorScores = new HashMap<>();
        float vectorMin = Float.MAX_VALUE, vectorMax = Float.MIN_VALUE;
        for (int i = 0; i < vectorResults.size(); i++) {
            Object contentObj = vectorResults.get(i).get("Content");
            Object scoreObj = vectorResults.get(i).get("Score");
            String content = (contentObj != null) ? contentObj.toString() : null;
            float score = 0.0f;
            if (scoreObj instanceof Number) {
                score = ((Number) scoreObj).floatValue();
            }
            if (content == null) continue;
            vectorRanks.put(content, i);
            vectorScores.put(content, score);
            vectorMin = Math.min(vectorMin, score);
            vectorMax = Math.max(vectorMax, score);
        }

        // Compute hybrid scores for all contents using normalized BM25 and vector scores
        Map<String, Double> hybridScores = new HashMap<>();
        for (String content : allContents) {
            // Normalize BM25 score to [0,1]
            double bm25Norm = 0.0;
            if (bm25Scores.containsKey(content) && bm25Max > bm25Min) {
                bm25Norm = (bm25Scores.get(content) - bm25Min) / (bm25Max - bm25Min);
            }

            // Normalize vector score to [0,1]
            double vectorNorm = 0.0;
            if (vectorScores.containsKey(content) && vectorMax > vectorMin) {
                vectorNorm = (vectorScores.get(content) - vectorMin) / (vectorMax - vectorMin);
            }

            // Weighted sum of normalized scores
            double finalScore = alpha * bm25Norm + beta * vectorNorm;
            hybridScores.put(content, finalScore);
        }

        // Build a map from content to its result map (preserving all fields)
        Map<String, Map<String, Object>> contentMap = new HashMap<>();
        for (Map<String, Object> result : bm25Results) {
            String content = (String) result.get("content");
            if (content != null) contentMap.put(content, new HashMap<>(result));
        }
        for (Map<String, Object> result : vectorResults) {
            Object contentObj = result.get("Content");
            String content = (contentObj != null) ? contentObj.toString() : null;
            if (content != null && !contentMap.containsKey(content)) {
                contentMap.put(content, new HashMap<>(result));
            }
        }

        // Sort contents by hybrid score descending
        List<Map.Entry<String, Double>> sorted = hybridScores.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .collect(Collectors.toList());

        // Build fused result list with top N entries, adding "hybrid_score"
        List<Map<String, Object>> fused = new ArrayList<>();
        for (int i = 0; i < Math.min(topN, sorted.size()); i++) {
            Map.Entry<String, Double> entry = sorted.get(i);
            Map<String, Object> doc = contentMap.get(entry.getKey());
            doc.put("hybrid_score", entry.getValue());
            fused.add(doc);
        }

        return fused;
    }
    
    /**
     * Analyzes the search prompt using simple NLP heuristics to dynamically adjust hybrid fusion weights.
     * 
     * The method tokenizes the input prompt and counts the number of stopwords versus total tokens.
     * Based on the ratio of stopwords to total tokens, it estimates whether the prompt is more likely
     * a keyword-based search or a natural language query, and sets the BM25 (lexical) and semantic weights accordingly.
     *
     *	CURRENT HEURISTIC -
     *  If the prompt is short and contains few stopwords, it favors BM25 (keyword search).
     *  If the prompt contains many stopwords or is more conversational, it favors semantic search.
     * 
     *
     * @param prompt The user's search prompt for the nearest neighbor call.
     * @return A double array where the first element is the BM25 weight (alpha) and the second is the semantic weight (beta).
     */
    static double[] getHybridWeights(String prompt) {
        // Default weights: alpha = BM25, beta = semantic
        double alpha = 0.35, beta = 0.65;

        // Simple NLP: if prompt is short and has few stopwords, favor BM25
        String[] stopwords = {"the", "is", "at", "which", "on", "a", "an", "and", "or", "for", "to"};
        int stopwordCount = 0;
        String[] tokens = prompt.toLowerCase().split("\\s+");
        for (String token : tokens) {
            for (String sw : stopwords) {
                if (token.equals(sw)) stopwordCount++;
            }
        }

        if (tokens.length <= 6 && stopwordCount <= 1) {
            // Likely a keyword search
            alpha = 0.65;
            beta = 0.35;
        } else if (stopwordCount > tokens.length / 2) {
            // More natural language, favor semantic
            alpha = 0.2;
            beta = 0.8;
        }
        return new double[]{alpha, beta};
    }
}
