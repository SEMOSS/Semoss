package prerna.engine.impl.vector;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class VectorRankingUtils {

    public static List<Map<String, Object>> rrfFuse(
            List<Map<String, Object>> bm25Results,
            List<Map<String, Object>> vectorResults,
            int topN) {

        Map<String, Double> rrfScores = new HashMap<>();
        int k = 30; // RRF constant

        BiConsumer<List<Map<String, Object>>, Integer> accumulate = (results, offset) -> {
            for (int i = 0; i < results.size(); i++) {
                String content = (String) results.get(i).get("content");
                if (content == null) continue;
                double score = 1.0 / (k + i + offset);
                double prevScore = rrfScores.getOrDefault(content, 0.0);
                rrfScores.put(content, prevScore + score);
            }
        };

        accumulate.accept(bm25Results, 0);
        accumulate.accept(vectorResults, 0);

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

        List<Map.Entry<String, Double>> sorted = rrfScores.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .collect(Collectors.toList());

        List<Map<String, Object>> fused = new ArrayList<>();
        for (int i = 0; i < Math.min(topN, sorted.size()); i++) {
            Map.Entry<String, Double> entry = sorted.get(i);
            Map<String, Object> doc = contentMap.get(entry.getKey());
            doc.put("rrf_score", entry.getValue());
            fused.add(doc);
        }

        return fused;
    }

    public static List<Map<String, Object>> hybridFuse(
            List<Map<String, Object>> bm25Results,
            List<Map<String, Object>> vectorResults,
            int topN,
            double alpha,  // weight for BM25 score
            double beta  // weight for semantic score
    ) {

        Set<String> allContents = new HashSet<>();
        for (Map<String, Object> r : bm25Results) allContents.add((String) r.get("content"));
        for (Map<String, Object> r : vectorResults) {
            Object contentObj = r.get("Content");
            if (contentObj != null) allContents.add(contentObj.toString());
        }

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

        Map<String, Double> hybridScores = new HashMap<>();
        for (String content : allContents) {
            double bm25Norm = 0.0;
            if (bm25Scores.containsKey(content) && bm25Max > bm25Min) {
                bm25Norm = (bm25Scores.get(content) - bm25Min) / (bm25Max - bm25Min);
            }

            double vectorNorm = 0.0;
            if (vectorScores.containsKey(content) && vectorMax > vectorMin) {
                vectorNorm = (vectorScores.get(content) - vectorMin) / (vectorMax - vectorMin);
            }

            double finalScore = alpha * bm25Norm + beta * vectorNorm;
            hybridScores.put(content, finalScore);
        }

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

        List<Map.Entry<String, Double>> sorted = hybridScores.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .collect(Collectors.toList());

        List<Map<String, Object>> fused = new ArrayList<>();
        for (int i = 0; i < Math.min(topN, sorted.size()); i++) {
            Map.Entry<String, Double> entry = sorted.get(i);
            Map<String, Object> doc = contentMap.get(entry.getKey());
            doc.put("hybrid_score", entry.getValue());
            fused.add(doc);
        }

        return fused;
    }
}
