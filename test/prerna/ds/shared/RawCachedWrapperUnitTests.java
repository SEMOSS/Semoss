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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;

public class RawCachedWrapperUnitTests {
	RawCachedWrapper reactor = new RawCachedWrapper();
	CachedIterator cachedIt;
	IHeadersDataRow dataRow;

	@Test
	void test() throws Exception {
		String[] headers = new String[]{"header"};
		SemossDataType[] types = new SemossDataType[1];

		cachedIt = mock(CachedIterator.class);
		dataRow = mock(IHeadersDataRow.class);

		when(cachedIt.getFirst()).thenReturn(true);
		when(cachedIt.hasNext()).thenReturn(true);
		when(cachedIt.next()).thenReturn(dataRow);
		when(cachedIt.getHeaders()).thenReturn(headers);
		when(cachedIt.getColTypes()).thenReturn(types);
		when(cachedIt.getInitSize()).thenReturn(1);

		assertNotNull(reactor.getIterator());

		reactor.setQuery("");
		reactor.setEngine(null);
		reactor.setIterator(cachedIt);
		reactor.execute();
		reactor.close();
		reactor.reset();

		assertTrue(reactor.first());
		assertEquals(cachedIt, reactor.getIterator());
		assertFalse(reactor.flushable());
		assertNull(reactor.flush());
		assertNull(reactor.getQuery());
		assertTrue(reactor.hasNext());
		assertEquals(dataRow, reactor.next());
		assertArrayEquals(headers, reactor.getHeaders());
		assertArrayEquals(types, reactor.getTypes());
		assertEquals(1, reactor.getNumRows());
		assertEquals(1, reactor.getNumRecords());
		assertNull(reactor.getEngine());
	}
}
