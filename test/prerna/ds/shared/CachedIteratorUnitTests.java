/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.ds.shared;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;

public class CachedIteratorUnitTests {
	String query;
	String[] headers;
	SemossDataType[] types;
	ITableDataFrame frame;
	IHeadersDataRow dataRow;

	CachedIterator reactor;

	@BeforeEach
	void setup() {
		reactor = new CachedIterator();

		query = "query";
		headers = new String[]{"array"};
		types = new SemossDataType[0];
		frame = mock(ITableDataFrame.class);
		dataRow = mock(IHeadersDataRow.class);
	}

	@Test
	void settersAndGetters() {
		reactor.setQuery(query);
		reactor.setHeaders(headers);
		reactor.setColTypes(types);
		reactor.setFrame(frame);

		assertNotNull(reactor.getQuery());
		assertNotNull(reactor.getHeaders());
		assertNotNull(reactor.getColTypes());
		assertNotNull(reactor.getFrame());
		assertNotNull(reactor.getFirst());
		assertEquals(0, reactor.getInitSize());
		assertFalse(reactor.hasNext());
	}

	@Test
	void test() {
		reactor.addNext(dataRow);
		reactor.addNext(mock(IHeadersDataRow.class));
		reactor.addJson("json1");
		reactor.addJson("json2");
		reactor.setFrame(frame);
		reactor.processCache();

		assertEquals(dataRow, reactor.next());
		assertEquals("json1", reactor.getNextJson());
		assertEquals("json1,json2", reactor.getAllJson());
		assertTrue(reactor.hasNext());
	}
}
