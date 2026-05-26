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

class PandasIteratorUnitTests {

    @Test
    void constructorWithValidData_storesFieldsCorrectly() {
        String[] headers = {"col1", "col2"};
        List<Object> data = new ArrayList<>(Arrays.asList(
                Arrays.asList("a", "b"),
                Arrays.asList("c", "d")
        ));
        SemossDataType[] types = {SemossDataType.STRING, SemossDataType.STRING};
        PandasIterator iter = new PandasIterator(headers, data, types);
        assertArrayEquals(headers, iter.getHeaders());
        assertSame(data, iter.fullData);
        assertArrayEquals(types, iter.colTypes);
        assertEquals(2, iter.getInitSize());
    }

    @Test
    void constructorWithNullFullData_initSizeIsZero() {
        String[] headers = {"col1"};
        SemossDataType[] types = {SemossDataType.INT};
        PandasIterator iter = new PandasIterator(headers, null, types);
        assertNotNull(iter.fullData);
        assertTrue(iter.fullData.isEmpty());
        assertEquals(0, iter.getInitSize());
    }

    @Test
    void hasNext_withData_returnsTrue() {
        List<Object> data = new ArrayList<>(List.of(Arrays.asList("x", "y")));
        PandasIterator iter = new PandasIterator(new String[]{"a", "b"}, data, null);
        assertTrue(iter.hasNext());
    }

    @Test
    void hasNext_withEmptyData_returnsFalse() {
        PandasIterator iter = new PandasIterator(new String[]{"a"}, new ArrayList<>(), null);
        assertFalse(iter.hasNext());
    }

    @Test
    void next_withListData_returnsHeadersDataRowWithCorrectValues() {
        String[] headers = {"name", "age"};
        List<Object> data = new ArrayList<>();
        data.add(Arrays.asList("Alice", 30));
        PandasIterator iter = new PandasIterator(headers, data, null);
        IHeadersDataRow row = iter.next();
        assertArrayEquals(headers, row.getHeaders());
        Object[] values = row.getValues();
        assertEquals("Alice", values[0]);
        assertEquals(30, values[1]);
    }

    @Test
    void next_withScalarData_returnsHeadersDataRowWithSingleValue() {
        String[] headers = {"count"};
        List<Object> data = new ArrayList<>();
        data.add(42);
        PandasIterator iter = new PandasIterator(headers, data, null);
        IHeadersDataRow row = iter.next();
        Object[] values = row.getValues();
        assertEquals(1, values.length);
        assertEquals(42, values[0]);
    }

    @Test
    void next_multipleListRows_iteratesAllRows() {
        String[] headers = {"x", "y"};
        List<Object> data = new ArrayList<>();
        data.add(Arrays.asList(1, 2));
        data.add(Arrays.asList(3, 4));
        data.add(Arrays.asList(5, 6));
        PandasIterator iter = new PandasIterator(headers, data, null);
        IHeadersDataRow row1 = iter.next();
        assertArrayEquals(new Object[]{1, 2}, row1.getValues());
        IHeadersDataRow row2 = iter.next();
        assertArrayEquals(new Object[]{3, 4}, row2.getValues());
        IHeadersDataRow row3 = iter.next();
        assertArrayEquals(new Object[]{5, 6}, row3.getValues());
    }

    @Test
    void next_multipleScalarRows_iteratesAllScalars() {
        String[] headers = {"val"};
        List<Object> data = new ArrayList<>(Arrays.asList("alpha", "beta", "gamma"));
        PandasIterator iter = new PandasIterator(headers, data, null);
        assertEquals("alpha", iter.next().getValues()[0]);
        assertEquals("beta", iter.next().getValues()[0]);
        assertEquals("gamma", iter.next().getValues()[0]);
    }

    @Test
    void next_removesElements_hasNextBecomesFalse() {
        List<Object> data = new ArrayList<>();
        data.add(Arrays.asList("a"));
        PandasIterator iter = new PandasIterator(new String[]{"h"}, data, null);
        assertTrue(iter.hasNext());
        iter.next();
        assertFalse(iter.hasNext());
    }

    @Test
    void next_withTransform_reordersValuesBasedOnActHeaders() {
        String[] headers = {"col1", "col2", "col3"};
        List<Object> data = new ArrayList<>();
        data.add(Arrays.asList("valC3", "valC1", "valC2"));
        PandasIterator iter = new PandasIterator(headers, data, null);
        iter.setTransform(Arrays.asList("col3", "col1", "col2"), true);
        IHeadersDataRow row = iter.next();
        Object[] vals = row.getValues();
        assertEquals("valC1", vals[0]);
        assertEquals("valC2", vals[1]);
        assertEquals("valC3", vals[2]);
    }

    @Test
    void next_withTransformAndMissingHeader_fallsBackToHeaderIndex() {
        String[] headers = {"col1", "col2"};
        List<Object> data = new ArrayList<>();
        data.add(Arrays.asList("A", "B"));
        PandasIterator iter = new PandasIterator(headers, data, null);
        iter.setTransform(Arrays.asList("col1"), true);
        IHeadersDataRow row = iter.next();
        Object[] vals = row.getValues();
        assertEquals("A", vals[0]);
        assertEquals("B", vals[1]);
    }

    @Test
    void getHeaders_returnsSameArrayFromConstructor() {
        String[] headers = {"h1", "h2", "h3"};
        PandasIterator iter = new PandasIterator(headers, null, null);
        assertSame(headers, iter.getHeaders());
    }

    @Test
    void getInitSize_returnsFullDataSizeAtConstruction() {
        List<Object> data = new ArrayList<>(Arrays.asList(
                Arrays.asList(1), Arrays.asList(2), Arrays.asList(3),
                Arrays.asList(4), Arrays.asList(5)
        ));
        PandasIterator iter = new PandasIterator(new String[]{"v"}, data, null);
        assertEquals(5, iter.getInitSize());
    }

    @Test
    void getInitSize_remainsUnchangedAfterIteration() {
        List<Object> data = new ArrayList<>(Arrays.asList(
                Arrays.asList(1), Arrays.asList(2), Arrays.asList(3)
        ));
        PandasIterator iter = new PandasIterator(new String[]{"v"}, data, null);
        iter.next();
        iter.next();
        assertEquals(3, iter.getInitSize());
        assertEquals(1, iter.fullData.size());
    }

    @Test
    void setQueryGetQuery_roundtrip() {
        PandasIterator iter = new PandasIterator(new String[]{"a"}, null, null);
        iter.setQuery("SELECT * FROM df");
        assertEquals("SELECT * FROM df", iter.getQuery());
    }

    @Test
    void getQuery_defaultIsNull() {
        PandasIterator iter = new PandasIterator(new String[]{"a"}, null, null);
        assertNull(iter.getQuery());
    }

    @Test
    void constructor_storesColTypes() {
        SemossDataType[] types = {SemossDataType.STRING, SemossDataType.INT, SemossDataType.DOUBLE};
        PandasIterator iter = new PandasIterator(new String[]{"a", "b", "c"}, null, types);
        assertSame(types, iter.colTypes);
        assertEquals(3, iter.colTypes.length);
        assertEquals(SemossDataType.STRING, iter.colTypes[0]);
        assertEquals(SemossDataType.INT, iter.colTypes[1]);
        assertEquals(SemossDataType.DOUBLE, iter.colTypes[2]);
    }

    @Test
    void fullIteration_hasNextReturnsFalseWhenExhausted() {
        List<Object> data = new ArrayList<>();
        data.add(Arrays.asList("r1c1", "r1c2"));
        data.add(Arrays.asList("r2c1", "r2c2"));
        PandasIterator iter = new PandasIterator(new String[]{"c1", "c2"}, data, null);
        assertTrue(iter.hasNext());
        iter.next();
        assertTrue(iter.hasNext());
        iter.next();
        assertFalse(iter.hasNext());
    }

    @Test
    void next_setsQueryOnReturnedRow() {
        List<Object> data = new ArrayList<>();
        data.add(Arrays.asList("val"));
        PandasIterator iter = new PandasIterator(new String[]{"col"}, data, null);
        iter.setQuery("SELECT col FROM table");
        IHeadersDataRow row = iter.next();
        assertEquals("SELECT col FROM table", row.getQuery());
    }

    @Test
    void next_withTransformFalse_doesNotReorder() {
        String[] headers = {"col1", "col2"};
        List<Object> data = new ArrayList<>();
        data.add(Arrays.asList("A", "B"));
        PandasIterator iter = new PandasIterator(headers, data, null);
        iter.setTransform(Arrays.asList("col2", "col1"), false);
        IHeadersDataRow row = iter.next();
        Object[] vals = row.getValues();
        assertEquals("A", vals[0]);
        assertEquals("B", vals[1]);
    }
}
