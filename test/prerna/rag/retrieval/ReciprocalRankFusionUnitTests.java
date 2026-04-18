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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

public class ReciprocalRankFusionUnitTests {

	private static final Function<Map<String, Object>, String> ID_EXTRACTOR = m -> (String) m.get("ID");

	@Test
	public void testFuseEmptyLists() {
		ReciprocalRankFusion rrf = new ReciprocalRankFusion();
		List<List<Map<String, Object>>> rankedLists = new ArrayList<>();
		List<Map<String, Object>> result = rrf.fuse(rankedLists, ID_EXTRACTOR, 10);
		assertNotNull(result);
		assertTrue(result.isEmpty());
	}

	@Test
	public void testFuseSingleList() {
		ReciprocalRankFusion rrf = new ReciprocalRankFusion();
		List<List<Map<String, Object>>> rankedLists = new ArrayList<>();

		List<Map<String, Object>> list1 = new ArrayList<>();
		list1.add(makeDoc("doc-a", "content a", "source1", 0.9));
		list1.add(makeDoc("doc-b", "content b", "source2", 0.8));
		rankedLists.add(list1);

		List<Map<String, Object>> result = rrf.fuse(rankedLists, ID_EXTRACTOR, 10);
		assertEquals(2, result.size());
		// First ranked doc should have highest RRF score
		assertEquals("doc-a", result.get(0).get("ID"));
	}

	@Test
	public void testFuseTwoLists_overlapping() {
		ReciprocalRankFusion rrf = new ReciprocalRankFusion();
		List<List<Map<String, Object>>> rankedLists = new ArrayList<>();

		// List 1: A ranked 1st, B ranked 2nd
		List<Map<String, Object>> list1 = new ArrayList<>();
		list1.add(makeDoc("doc-a", "content a", "source1", 0.9));
		list1.add(makeDoc("doc-b", "content b", "source2", 0.8));
		rankedLists.add(list1);

		// List 2: B ranked 1st, C ranked 2nd
		List<Map<String, Object>> list2 = new ArrayList<>();
		list2.add(makeDoc("doc-b", "content b", "source2", 0.95));
		list2.add(makeDoc("doc-c", "content c", "source3", 0.7));
		rankedLists.add(list2);

		List<Map<String, Object>> result = rrf.fuse(rankedLists, ID_EXTRACTOR, 10);
		assertEquals(3, result.size());

		// doc-b appears in both lists (rank 1 in list1 + rank 1 in list2 gives highest combined)
		// So doc-b should be first
		assertEquals("doc-b", result.get(0).get("ID"));
	}

	@Test
	public void testFuseRespectsLimit() {
		ReciprocalRankFusion rrf = new ReciprocalRankFusion();
		List<List<Map<String, Object>>> rankedLists = new ArrayList<>();

		List<Map<String, Object>> list1 = new ArrayList<>();
		for (int i = 0; i < 20; i++) {
			list1.add(makeDoc("doc-" + i, "content " + i, "source", 1.0 - i * 0.01));
		}
		rankedLists.add(list1);

		List<Map<String, Object>> result = rrf.fuse(rankedLists, ID_EXTRACTOR, 5);
		assertEquals(5, result.size());
	}

	@Test
	public void testFuseByContent() {
		ReciprocalRankFusion rrf = new ReciprocalRankFusion();

		// Same content, different IDs — fuseByContent merges by content
		List<Map<String, Object>> vectorResults = new ArrayList<>();
		vectorResults.add(makeDoc("id-1", "shared content", "source1", 0.9));

		List<Map<String, Object>> keywordResults = new ArrayList<>();
		keywordResults.add(makeDoc("id-2", "shared content", "source1", 0.85));

		List<Map<String, Object>> result = rrf.fuseByContent(vectorResults, keywordResults, 10);
		// Should merge into one because content is the same
		assertEquals(1, result.size());
	}

	@Test
	public void testCustomK() {
		ReciprocalRankFusion rrf = new ReciprocalRankFusion(30);
		List<List<Map<String, Object>>> rankedLists = new ArrayList<>();

		List<Map<String, Object>> list1 = new ArrayList<>();
		list1.add(makeDoc("doc-a", "content a", "source1", 0.9));
		rankedLists.add(list1);

		List<Map<String, Object>> result = rrf.fuse(rankedLists, ID_EXTRACTOR, 10);
		assertNotNull(result);
		assertEquals(1, result.size());
		// With k=30, RRF score for rank 1 = 1/(30+0+1) = 1/31 ≈ 0.0323
		double score = (Double) result.get(0).get("Score");
		assertEquals(1.0 / 31.0, score, 0.0001);
	}

	private Map<String, Object> makeDoc(String id, String content, String source, double score) {
		Map<String, Object> doc = new HashMap<>();
		doc.put("ID", id);
		doc.put("Content", content);
		doc.put("Source", source);
		doc.put("Score", score);
		return doc;
	}
}
