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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.weaviate.client6.v1.api.collections.query.Filter;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;

/**
 * Verifies that SEMOSS filters translate into (non-null) Weaviate filters. The
 * Weaviate {@link Filter} has no public accessors for its internals, so these
 * tests assert that translation succeeds/short-circuits rather than the exact
 * shape produced.
 */
public class WeaviateVectorQueryFilterTranslationHelperTest {

	@Test
	void testNullAndEmptyReturnNull() {
		assertNull(WeaviateVectorQueryFilterTranslationHelper.translate(null));
		assertNull(WeaviateVectorQueryFilterTranslationHelper.translate(new ArrayList<>()));
	}

	@Test
	void testSingleEqualsFilter() {
		List<IQueryFilter> filters = Collections
				.singletonList(SimpleQueryFilter.makeColToValFilter("Source", "==", "somatosensory.pdf"));
		assertNotNull(WeaviateVectorQueryFilterTranslationHelper.translate(filters));
	}

	@Test
	void testEqualsListOfValuesFilter() {
		List<IQueryFilter> filters = Collections.singletonList(
				SimpleQueryFilter.makeColToValFilter("Source", "==", Arrays.asList("somatosensory.pdf", "test.pdf")));
		assertNotNull(WeaviateVectorQueryFilterTranslationHelper.translate(filters));
	}

	@Test
	void testNotEqualsFilter() {
		List<IQueryFilter> filters = Collections
				.singletonList(SimpleQueryFilter.makeColToValFilter("Modality", "!=", "text"));
		assertNotNull(WeaviateVectorQueryFilterTranslationHelper.translate(filters));
	}

	@Test
	void testRangeAndLikeFilters() {
		assertNotNull(WeaviateVectorQueryFilterTranslationHelper.translate(
				Collections.singletonList(SimpleQueryFilter.makeColToValFilter("Divider", ">", "1"))));
		assertNotNull(WeaviateVectorQueryFilterTranslationHelper.translate(
				Collections.singletonList(SimpleQueryFilter.makeColToValFilter("Source", "?like", "soma"))));
		assertNotNull(WeaviateVectorQueryFilterTranslationHelper.translate(
				Collections.singletonList(SimpleQueryFilter.makeColToValFilter("Source", "?begins", "soma"))));
	}

	@Test
	void testAndFilter() {
		List<IQueryFilter> nested = Arrays.asList(SimpleQueryFilter.makeColToValFilter("Modality", "==", "text"),
				SimpleQueryFilter.makeColToValFilter("Divider", "==", "1"));
		List<IQueryFilter> filters = Collections.singletonList(new AndQueryFilter(nested));
		assertNotNull(WeaviateVectorQueryFilterTranslationHelper.translate(filters));
	}

	@Test
	void testOrFilter() {
		List<IQueryFilter> nested = Arrays.asList(SimpleQueryFilter.makeColToValFilter("Source", "==", "somatosensory.pdf"),
				SimpleQueryFilter.makeColToValFilter("Modality", "==", "text"));
		List<IQueryFilter> filters = Collections.singletonList(new OrQueryFilter(nested));
		assertNotNull(WeaviateVectorQueryFilterTranslationHelper.translate(filters));
	}

	@Test
	void testUnsupportedComparatorThrows() {
		List<IQueryFilter> filters = Collections
				.singletonList(SimpleQueryFilter.makeColToValFilter("Source", "?unknown", "x"));
		assertThrows(IllegalArgumentException.class,
				() -> WeaviateVectorQueryFilterTranslationHelper.translate(filters));
	}
}
