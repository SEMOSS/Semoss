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

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;

class PandasParquetIteratorUnitTests {

    // ---- Constructor / preprocess tests per type ----

    @Test
    void constructorWithLongArrayColumn() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.INT};
        List<Object> blob = new ArrayList<>();
        blob.add(new long[]{10L, 20L, 30L});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        assertNotNull(iter.finalList);
        assertEquals(1, iter.finalList.length);
        assertArrayEquals(new String[]{"long"}, iter.finalTypes);
        assertEquals(3, iter.finalSize);
    }

    @Test
    void constructorWithIntArrayColumn() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.INT};
        List<Object> blob = new ArrayList<>();
        blob.add(new int[]{1, 2, 3, 4});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        assertNotNull(iter.finalList);
        assertEquals(1, iter.finalList.length);
        assertArrayEquals(new String[]{"int"}, iter.finalTypes);
        assertEquals(4, iter.finalSize);
    }

    @Test
    void constructorWithFloatArrayColumn() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.DOUBLE};
        List<Object> blob = new ArrayList<>();
        blob.add(new float[]{1.1f, 2.2f});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        assertNotNull(iter.finalList);
        assertEquals(1, iter.finalList.length);
        assertArrayEquals(new String[]{"float"}, iter.finalTypes);
        assertEquals(2, iter.finalSize);
    }

    @Test
    void constructorWithDoubleArrayColumn() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.DOUBLE};
        List<Object> blob = new ArrayList<>();
        blob.add(new double[]{3.14, 2.71, 1.41});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        assertNotNull(iter.finalList);
        assertEquals(1, iter.finalList.length);
        assertArrayEquals(new String[]{"double"}, iter.finalTypes);
        assertEquals(3, iter.finalSize);
    }

    @Test
    void constructorWithListColumn() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.STRING};
        List<Object> blob = new ArrayList<>();
        List<String> col = Arrays.asList("a", "b", "c");
        blob.add(col);

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        assertNotNull(iter.finalList);
        assertEquals(1, iter.finalList.length);
        assertArrayEquals(new String[]{"list"}, iter.finalTypes);
        assertEquals(3, iter.finalSize);
    }

    @Test
    void constructorWithMixedColumnTypes() {
        String[] headers = {"longs", "strings", "doubles"};
        SemossDataType[] types = {SemossDataType.INT, SemossDataType.STRING, SemossDataType.DOUBLE};
        List<Object> blob = new ArrayList<>();
        blob.add(new long[]{100L, 200L});
        blob.add(Arrays.asList("x", "y"));
        blob.add(new double[]{1.5, 2.5});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        assertEquals(3, iter.finalList.length);
        assertEquals("long", iter.finalTypes[0]);
        assertEquals("list", iter.finalTypes[1]);
        assertEquals("double", iter.finalTypes[2]);
        assertEquals(2, iter.finalSize);
    }

    // ---- hasNext tests ----

    @Test
    void hasNextTrueWhenCursorLessThanFinalSize() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.INT};
        List<Object> blob = new ArrayList<>();
        blob.add(new long[]{1L, 2L, 3L});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        assertTrue(iter.hasNext());
    }

    @Test
    void hasNextFalseWhenCursorEqualsFinalSize() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.INT};
        List<Object> blob = new ArrayList<>();
        blob.add(new long[]{1L});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);
        iter.cursor = iter.finalSize;

        assertFalse(iter.hasNext());
    }

    // ---- next() returns correct values per type ----

    @Test
    void nextReturnsCorrectValuesFromLongArray() {
        String[] headers = {"longs"};
        SemossDataType[] types = {SemossDataType.INT};
        List<Object> blob = new ArrayList<>();
        blob.add(new long[]{42L, 99L});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);
        IHeadersDataRow row = iter.next();

        Object[] values = row.getValues();
        assertEquals(1, values.length);
        assertEquals(42L, values[0]);
    }

    @Test
    void nextReturnsCorrectValuesFromIntArray() {
        String[] headers = {"ints"};
        SemossDataType[] types = {SemossDataType.INT};
        List<Object> blob = new ArrayList<>();
        blob.add(new int[]{7, 8, 9});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);
        IHeadersDataRow row = iter.next();

        Object[] values = row.getValues();
        assertEquals(1, values.length);
        assertEquals(7, values[0]);
    }

    @Test
    void nextReturnsCorrectValuesFromFloatArray() {
        String[] headers = {"floats"};
        SemossDataType[] types = {SemossDataType.DOUBLE};
        List<Object> blob = new ArrayList<>();
        blob.add(new float[]{1.5f, 2.5f});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);
        IHeadersDataRow row = iter.next();

        Object[] values = row.getValues();
        assertEquals(1, values.length);
        assertEquals(1.5f, values[0]);
    }

    @Test
    void nextReturnsCorrectValuesFromDoubleArray() {
        String[] headers = {"doubles"};
        SemossDataType[] types = {SemossDataType.DOUBLE};
        List<Object> blob = new ArrayList<>();
        blob.add(new double[]{3.14, 2.71});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);
        IHeadersDataRow row = iter.next();

        Object[] values = row.getValues();
        assertEquals(1, values.length);
        assertEquals(3.14, values[0]);
    }

    @Test
    void nextReturnsCorrectValuesFromList() {
        String[] headers = {"strings"};
        SemossDataType[] types = {SemossDataType.STRING};
        List<Object> blob = new ArrayList<>();
        blob.add(Arrays.asList("hello", "world"));

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);
        IHeadersDataRow row = iter.next();

        Object[] values = row.getValues();
        assertEquals(1, values.length);
        assertEquals("hello", values[0]);
    }

    // ---- Iteration tests ----

    @Test
    void iterateThroughAllRows() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.INT};
        List<Object> blob = new ArrayList<>();
        blob.add(new long[]{10L, 20L, 30L});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        List<Object> collected = new ArrayList<>();
        while (iter.hasNext()) {
            IHeadersDataRow row = iter.next();
            collected.add(row.getValues()[0]);
        }

        assertEquals(3, collected.size());
        assertEquals(10L, collected.get(0));
        assertEquals(20L, collected.get(1));
        assertEquals(30L, collected.get(2));
    }

    @Test
    void cursorIncrementsAfterEachNext() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.INT};
        List<Object> blob = new ArrayList<>();
        blob.add(new long[]{1L, 2L, 3L});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        assertEquals(0, iter.cursor);
        iter.next();
        assertEquals(1, iter.cursor);
        iter.next();
        assertEquals(2, iter.cursor);
        iter.next();
        assertEquals(3, iter.cursor);
    }

    // ---- getHeaders / getInitSize ----

    @Test
    void getHeadersReturnsHeaders() {
        String[] headers = {"a", "b", "c"};
        SemossDataType[] types = {SemossDataType.INT, SemossDataType.STRING, SemossDataType.DOUBLE};
        List<Object> blob = new ArrayList<>();
        blob.add(new long[]{1L});
        blob.add(Arrays.asList("x"));
        blob.add(new double[]{1.0});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        assertArrayEquals(new String[]{"a", "b", "c"}, iter.getHeaders());
    }

    @Test
    void getInitSizeReturnsFinalSize() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.INT};
        List<Object> blob = new ArrayList<>();
        blob.add(new int[]{1, 2, 3, 4, 5});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        assertEquals(5, iter.getInitSize());
    }

    // ---- setQuery / getQuery ----

    @Test
    void setQueryGetQueryRoundtrip() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.INT};
        List<Object> blob = new ArrayList<>();
        blob.add(new long[]{1L});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);
        iter.setQuery("SELECT * FROM table");

        assertEquals("SELECT * FROM table", iter.getQuery());
    }

    @Test
    void getQueryNullByDefault() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.INT};
        List<Object> blob = new ArrayList<>();
        blob.add(new long[]{1L});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        assertNull(iter.getQuery());
    }

    // ---- setTransform ----

    @Test
    void setTransformStoresValues() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.INT};
        List<Object> blob = new ArrayList<>();
        blob.add(new long[]{1L});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        // setTransform should not throw
        assertDoesNotThrow(() -> iter.setTransform(Arrays.asList("col1"), true));
    }

    // ---- Constructor with non-List blob ----

    @Test
    void constructorWithNonListBlobFinalListStaysNull() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.INT};
        // Pass a non-List object as the blob
        Object nonListBlob = "not a list";

        PandasParquetIterator iter = new PandasParquetIterator(headers, nonListBlob, types);

        assertNull(iter.finalList);
    }

    // ---- Multiple rows with mixed types ----

    @Test
    void multipleRowsVerifyEachRow() {
        String[] headers = {"id", "name", "score"};
        SemossDataType[] types = {SemossDataType.INT, SemossDataType.STRING, SemossDataType.DOUBLE};
        List<Object> blob = new ArrayList<>();
        blob.add(new long[]{1L, 2L, 3L});
        blob.add(Arrays.asList("Alice", "Bob", "Charlie"));
        blob.add(new double[]{95.5, 87.3, 92.1});

        PandasParquetIterator iter = new PandasParquetIterator(headers, blob, types);

        // Row 0
        assertTrue(iter.hasNext());
        IHeadersDataRow row0 = iter.next();
        Object[] vals0 = row0.getValues();
        assertEquals(1L, vals0[0]);
        assertEquals("Alice", vals0[1]);
        assertEquals(95.5, vals0[2]);

        // Row 1
        assertTrue(iter.hasNext());
        IHeadersDataRow row1 = iter.next();
        Object[] vals1 = row1.getValues();
        assertEquals(2L, vals1[0]);
        assertEquals("Bob", vals1[1]);
        assertEquals(87.3, vals1[2]);

        // Row 2
        assertTrue(iter.hasNext());
        IHeadersDataRow row2 = iter.next();
        Object[] vals2 = row2.getValues();
        assertEquals(3L, vals2[0]);
        assertEquals("Charlie", vals2[1]);
        assertEquals(92.1, vals2[2]);

        // No more rows
        assertFalse(iter.hasNext());
    }

}
