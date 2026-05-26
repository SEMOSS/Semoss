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
package prerna.ds.py;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;

class RawPandasWrapperUnitTests {

    private RawPandasWrapper wrapper;
    private PandasIterator iterator;

    private PandasIterator createIterator(String[] headers, List<Object> data, SemossDataType[] colTypes) {
        PandasIterator pi = new PandasIterator(headers, data, colTypes);
        return pi;
    }

    @BeforeEach
    void setUp() {
        String[] headers = new String[]{"col1", "col2"};
        List<Object> data = new ArrayList<>();
        data.add(Arrays.asList("a", "b"));
        data.add(Arrays.asList("c", "d"));
        SemossDataType[] colTypes = new SemossDataType[]{SemossDataType.STRING, SemossDataType.STRING};

        iterator = createIterator(headers, data, colTypes);
        iterator.setQuery("SELECT * FROM table");

        wrapper = new RawPandasWrapper();
        wrapper.setPandasIterator(iterator);
    }

    @Test
    void testSetPandasIteratorStoresIterator() {
        RawPandasWrapper w = new RawPandasWrapper();

        PandasIterator pi = createIterator(new String[]{"h1"}, new ArrayList<>(), new SemossDataType[]{SemossDataType.INT});
        w.setPandasIterator(pi);

        // After setting, getHeaders should delegate to the iterator we just set
        assertArrayEquals(new String[]{"h1"}, w.getHeaders());
    }

    @Test
    void testHasNextDelegatesToIterator() {
        // iterator has 2 rows, so hasNext should be true
        assertTrue(wrapper.hasNext());
    }

    @Test
    void testHasNextReturnsFalseWhenEmpty() {
        PandasIterator emptyIterator = createIterator(new String[]{"col1"}, new ArrayList<>(), new SemossDataType[]{SemossDataType.STRING});
        RawPandasWrapper w = new RawPandasWrapper();
        w.setPandasIterator(emptyIterator);

        assertFalse(w.hasNext());
    }

    @Test
    void testNextDelegatesToIterator() {
        IHeadersDataRow row = wrapper.next();
        assertNotNull(row);
        assertArrayEquals(new String[]{"col1", "col2"}, row.getHeaders());
        Object[] values = row.getValues();
        assertEquals("a", values[0]);
        assertEquals("b", values[1]);
    }

    @Test
    void testNextConsumesRows() {
        wrapper.next(); // consume first row
        wrapper.next(); // consume second row
        assertFalse(wrapper.hasNext());
    }

    @Test
    void testGetHeadersDelegatesToIterator() {
        String[] headers = wrapper.getHeaders();
        assertArrayEquals(new String[]{"col1", "col2"}, headers);
    }

    @Test
    void testGetTypesReturnsIteratorColTypes() {
        SemossDataType[] types = wrapper.getTypes();
        assertNotNull(types);
        assertEquals(2, types.length);
        assertEquals(SemossDataType.STRING, types[0]);
        assertEquals(SemossDataType.STRING, types[1]);
    }

    @Test
    void testGetQueryDelegatesToIterator() {
        assertEquals("SELECT * FROM table", wrapper.getQuery());
    }

    @Test
    void testGetNumRowsReturnsInitSize() {
        // initSize is set from fullData.size() at construction time = 2
        assertEquals(2, wrapper.getNumRows());
    }

    @Test
    void testGetNumRecordsReturnsInitSizeTimesHeadersLength() {
        // initSize=2, headers.length=2 => 4
        assertEquals(4, wrapper.getNumRecords());
    }

    @Test
    void testExecuteIsNoOp() {
        assertDoesNotThrow(() -> wrapper.execute());
    }

    @Test
    void testCloseIsNoOp() {
        assertDoesNotThrow(() -> wrapper.close());
    }

    @Test
    void testSetQueryIsNoOp() {
        assertDoesNotThrow(() -> wrapper.setQuery("anything"));
        // The wrapper's setQuery is a no-op, the query comes from the iterator
        assertEquals("SELECT * FROM table", wrapper.getQuery());
    }

    @Test
    void testSetEngineIsNoOp() {
        assertDoesNotThrow(() -> wrapper.setEngine(null));
    }

    @Test
    void testResetIsNoOp() {
        assertDoesNotThrow(() -> wrapper.reset());
    }

    @Test
    void testGetEngineReturnsNull() {
        assertNull(wrapper.getEngine());
    }

    @Test
    void testFlushableReturnsFalse() {
        assertFalse(wrapper.flushable());
    }

    @Test
    void testFlushReturnsNull() {
        assertNull(wrapper.flush());
    }
}
