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

class RawPandasParquetWrapperUnitTests {

    private RawPandasParquetWrapper wrapper;
    private PandasParquetIterator iterator;

    private PandasParquetIterator createIterator(String[] headers, Object blob, SemossDataType[] colTypes) {
        return new PandasParquetIterator(headers, blob, colTypes);
    }

    @BeforeEach
    void setUp() {
        String[] headers = new String[]{"col1", "col2"};
        // Blob is a list of columns; each column is a list of values
        List<Object> blob = new ArrayList<>();
        blob.add(Arrays.asList("a", "c")); // col1 values
        blob.add(Arrays.asList("b", "d")); // col2 values
        SemossDataType[] colTypes = new SemossDataType[]{SemossDataType.STRING, SemossDataType.STRING};

        iterator = createIterator(headers, blob, colTypes);
        iterator.setQuery("SELECT * FROM parquet_table");

        wrapper = new RawPandasParquetWrapper();
        wrapper.setPandasParquetIterator(iterator);
    }

    @Test
    void testSetPandasParquetIteratorStoresIterator() {
        RawPandasParquetWrapper w = new RawPandasParquetWrapper();

        List<Object> blob = new ArrayList<>();
        blob.add(Arrays.asList("x"));
        PandasParquetIterator pi = createIterator(new String[]{"h1"}, blob, new SemossDataType[]{SemossDataType.INT});
        w.setPandasParquetIterator(pi);

        // After setting, getHeaders should delegate to the iterator
        assertArrayEquals(new String[]{"h1"}, w.getHeaders());
    }

    @Test
    void testHasNextDelegatesToIterator() {
        // iterator has 2 rows, so hasNext should be true
        assertTrue(wrapper.hasNext());
    }

    @Test
    void testHasNextReturnsFalseWhenExhausted() {
        wrapper.next(); // consume row 0
        wrapper.next(); // consume row 1
        assertFalse(wrapper.hasNext());
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
    void testNextConsumesRowsSequentially() {
        IHeadersDataRow row1 = wrapper.next();
        assertEquals("a", row1.getValues()[0]);
        assertEquals("b", row1.getValues()[1]);

        IHeadersDataRow row2 = wrapper.next();
        assertEquals("c", row2.getValues()[0]);
        assertEquals("d", row2.getValues()[1]);

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
        assertEquals("SELECT * FROM parquet_table", wrapper.getQuery());
    }

    @Test
    void testGetNumRowsReturnsInitSize() {
        // finalSize is set from blob list element sizes = 2
        assertEquals(2, wrapper.getNumRows());
    }

    @Test
    void testGetNumRecordsReturnsInitSizeTimesHeadersLength() {
        // finalSize=2, headers.length=2 => 4
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
        assertEquals("SELECT * FROM parquet_table", wrapper.getQuery());
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
