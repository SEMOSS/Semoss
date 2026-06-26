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
package prerna.engine.impl.vector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory BM25 keyword index for a Chroma collection. Chroma OSS has no usable native keyword
 * search over REST, so this provides it app-side. The index is derived from the chunk {@code Content}
 * that already lives in Chroma — it holds no separate persisted state and is rebuilt from the
 * collection on engine open. Thread-safe via a read/write lock.
 */
public class ChromaBm25Index {

	private static final double BM25_K1 = 1.2;
	private static final double BM25_B = 0.75;

	/** Key under which {@link #search} tags each result row with its chunk id (for fusion/dedup). */
	public static final String ID_KEY = "_bm25_id";

	/** One indexed chunk: enough to score it and return it without re-fetching from Chroma. */
	private static class Record {
		private String source;
		private Map<String, Object> metadata;
		private Map<String, Integer> termFreqs;
		private int length;
	}

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final Map<String, Record> records = new LinkedHashMap<>();
	private Map<String, Integer> docFreq = new LinkedHashMap<>();
	private double avgDocLength = 0.0;
	private boolean statsDirty = true;

	public boolean isEmpty() {
		lock.readLock().lock();
		try {
			return records.isEmpty();
		} finally {
			lock.readLock().unlock();
		}
	}

	/** Add (or replace) one chunk. {@code metadata} is copied so later mutations don't leak in. */
	public void addRecord(String id, String source, String content, Map<String, Object> metadata) {
		Record record = new Record();
		record.source = source;
		record.metadata = (metadata != null) ? new LinkedHashMap<>(metadata) : new LinkedHashMap<>();
		record.termFreqs = new LinkedHashMap<>();
		List<String> tokens = tokenize(content);
		for (String token : tokens) {
			record.termFreqs.merge(token, 1, Integer::sum);
		}
		record.length = tokens.size();

		lock.writeLock().lock();
		try {
			records.put(id, record);
			statsDirty = true;
		} finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * Remove every chunk whose source matches.
	 *
	 * @return the number of chunks removed
	 */
	public int removeBySource(String source) {
		lock.writeLock().lock();
		try {
			int before = records.size();
			records.values().removeIf(r -> source.equals(r.source));
			int removed = before - records.size();
			if (removed > 0) {
				statsDirty = true;
			}
			return removed;
		} finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * Score the query against the full corpus and return the top-{@code topK} chunks, best first.
	 * Each result is a copy of the chunk's metadata plus its BM25 {@code Score} and {@link #ID_KEY};
	 * only chunks matching at least one query term are returned.
	 */
	public List<Map<String, Object>> search(String query, int topK) {
		List<String> queryTerms = uniqueTokens(query);
		List<Map<String, Object>> results = new ArrayList<>();
		if (queryTerms.isEmpty()) {
			return results;
		}

		ensureStats();
		lock.readLock().lock();
		try {
			if (records.isEmpty() || avgDocLength <= 0.0) {
				return results;
			}
			int n = records.size();
			for (Map.Entry<String, Record> entry : records.entrySet()) {
				double score = scoreRecord(entry.getValue(), queryTerms, n);
				if (score <= 0.0) {
					continue;
				}
				Map<String, Object> row = new LinkedHashMap<>(entry.getValue().metadata);
				row.put(ID_KEY, entry.getKey());
				row.put("Score", score);
				results.add(row);
			}
			results.sort((a, b) -> Double.compare((double) b.get("Score"), (double) a.get("Score")));
			return results.subList(0, Math.min(topK, results.size()));
		} finally {
			lock.readLock().unlock();
		}
	}

	private double scoreRecord(Record record, List<String> queryTerms, int n) {
		double score = 0.0;
		for (String term : queryTerms) {
			Integer f = record.termFreqs.get(term);
			if (f == null) {
				continue;
			}
			int df = docFreq.getOrDefault(term, 0);
			double idf = Math.log(1.0 + (n - df + 0.5) / (df + 0.5));
			double denom = f + BM25_K1 * (1.0 - BM25_B + BM25_B * record.length / avgDocLength);
			score += idf * (f * (BM25_K1 + 1.0)) / denom;
		}
		return score;
	}

	/**
	 * Recompute corpus statistics (document frequency, average length) when the index has changed.
	 * Runs the rebuild under the write lock — never under a read lock — so concurrent searches can't
	 * race on the shared stats. Double-checked so only one rebuild happens per change.
	 */
	private void ensureStats() {
		lock.readLock().lock();
		boolean dirty;
		try {
			dirty = statsDirty;
		} finally {
			lock.readLock().unlock();
		}
		if (!dirty) {
			return;
		}
		lock.writeLock().lock();
		try {
			if (!statsDirty) {
				return;
			}
			Map<String, Integer> freshDocFreq = new LinkedHashMap<>();
			long totalLength = 0;
			for (Record record : records.values()) {
				totalLength += record.length;
				for (String term : record.termFreqs.keySet()) {
					freshDocFreq.merge(term, 1, Integer::sum);
				}
			}
			this.docFreq = freshDocFreq;
			this.avgDocLength = records.isEmpty() ? 0.0 : (double) totalLength / records.size();
			this.statsDirty = false;
		} finally {
			lock.writeLock().unlock();
		}
	}

	/** Lower-case and split into alphanumeric tokens (no external tokenizer/stemmer). */
	public static List<String> tokenize(String text) {
		List<String> tokens = new ArrayList<>();
		if (text == null || text.isEmpty()) {
			return tokens;
		}
		for (String token : text.toLowerCase().split("[^a-z0-9]+")) {
			if (!token.isEmpty()) {
				tokens.add(token);
			}
		}
		return tokens;
	}

	private static List<String> uniqueTokens(String text) {
		return new ArrayList<>(new LinkedHashSet<>(tokenize(text)));
	}
}
