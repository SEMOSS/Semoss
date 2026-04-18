/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.rag.retrieval;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Reciprocal Rank Fusion (RRF) implementation for combining results from
 * multiple retrieval systems (e.g., dense vector search + sparse BM25 keyword search).
 * 
 * RRF formula: score(d) = Σ 1 / (k + rank_i(d))
 * where k is a constant (default 60) and rank_i(d) is the rank of document d in result list i.
 * 
 * Reference: Cormack, Clarke, Buettcher (2009) - "Reciprocal Rank Fusion outperforms
 * Condorcet and individual Rank Learning Methods"
 */
public class ReciprocalRankFusion {

	private static final int DEFAULT_K = 60;

	private final int k;

	public ReciprocalRankFusion() {
		this.k = DEFAULT_K;
	}

	public ReciprocalRankFusion(int k) {
		if (k < 1) {
			throw new IllegalArgumentException("RRF constant k must be >= 1, got " + k);
		}
		this.k = k;
	}

	/**
	 * Fuse multiple ranked result lists using Reciprocal Rank Fusion.
	 * Each result is identified by a key extracted via the keyExtractor function.
	 *
	 * @param rankedLists  list of ranked result lists (each pre-sorted by relevance descending)
	 * @param keyExtractor function to extract a unique key from each result map
	 * @param limit        max results to return
	 * @return fused results sorted by RRF score descending, with "Score" and "ScoreType" fields added
	 */
	public List<Map<String, Object>> fuse(
			List<List<Map<String, Object>>> rankedLists,
			Function<Map<String, Object>, String> keyExtractor,
			int limit) {

		// key -> accumulated RRF score
		Map<String, Double> rrfScores = new HashMap<>();
		// key -> best result map (keep the one from the highest-scored list)
		Map<String, Map<String, Object>> resultData = new HashMap<>();

		for (List<Map<String, Object>> rankedList : rankedLists) {
			for (int rank = 0; rank < rankedList.size(); rank++) {
				Map<String, Object> result = rankedList.get(rank);
				String key = keyExtractor.apply(result);
				if (key == null) {
					continue;
				}

				double rrfContribution = 1.0 / (this.k + rank + 1);
				rrfScores.merge(key, rrfContribution, Double::sum);

				// keep the result data from the first list that contains it
				resultData.putIfAbsent(key, result);
			}
		}

		// build fused results
		List<Map<String, Object>> fusedResults = new ArrayList<>();
		for (Map.Entry<String, Double> entry : rrfScores.entrySet()) {
			Map<String, Object> result = new HashMap<>(resultData.get(entry.getKey()));
			result.put("Score", entry.getValue());
			result.put("ScoreType", "rrf");
			fusedResults.add(result);
		}

		// sort by RRF score descending
		fusedResults.sort(Comparator.comparingDouble(
				(Map<String, Object> m) -> ((Number) m.get("Score")).doubleValue()).reversed());

		if (fusedResults.size() > limit) {
			return fusedResults.subList(0, limit);
		}
		return fusedResults;
	}

	/**
	 * Convenience method for fusing two result lists using content as key.
	 */
	public List<Map<String, Object>> fuseByContent(
			List<Map<String, Object>> vectorResults,
			List<Map<String, Object>> keywordResults,
			int limit) {

		List<List<Map<String, Object>>> rankedLists = new ArrayList<>();
		rankedLists.add(vectorResults);
		rankedLists.add(keywordResults);

		return fuse(rankedLists, result -> {
			Object content = result.getOrDefault("Content", result.get("CONTENT"));
			if (content == null) {
				content = result.getOrDefault("Source", result.get("SOURCE"));
			}
			return content != null ? content.toString() : null;
		}, limit);
	}

	public int getK() {
		return k;
	}

}
