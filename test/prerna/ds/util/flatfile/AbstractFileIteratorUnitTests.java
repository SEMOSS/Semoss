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
package prerna.ds.util.flatfile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IDatabaseEngine;

public class AbstractFileIteratorUnitTests {

    private AbstractFileIterator fileIterator;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        fileIterator = Mockito.mock(AbstractFileIterator.class, Mockito.CALLS_REAL_METHODS);
        fileIterator.fileLocation = "test.csv";
        fileIterator.headers = new String[] { "col1", "col2", "col3" };
        fileIterator.types = new SemossDataType[] { SemossDataType.INT, SemossDataType.DOUBLE, SemossDataType.BOOLEAN };
        fileIterator.additionalTypes = new String[] { null, null, null };
        fileIterator.nextRow = new Object[] { "1", "2.0", "true" };
    }

    @Test
    void testHasNextWithOffset() throws Exception {
        fileIterator.offset = 3;
        Mockito.doAnswer(invocation -> {
            if (fileIterator.curOffset < 5) {
                fileIterator.nextRow = new Object[] { "1", "2.0", "true" };
                fileIterator.curOffset++;
            } else {
                fileIterator.nextRow = null;
            }
            return null;
        }).when(fileIterator).getNextRow();
        assertTrue(fileIterator.hasNext());
    }

    @Test
    void testHasNextWithLimit() throws IOException, Exception {
        fileIterator.limit = 2;
        Mockito.doAnswer(invocation -> {
            if (fileIterator.curOffset < 5) {
                fileIterator.nextRow = new Object[] { "1", "2.0", "true" };
                fileIterator.curOffset++;
            } else {
                fileIterator.nextRow = null;
            }
            return null;
        }).when(fileIterator).getNextRow();
        assertTrue(fileIterator.hasNext());
        fileIterator.getNextRow();
        assertTrue(fileIterator.hasNext());
        fileIterator.getNextRow();
    }

    @Test
    void testHasNextWithCurLimitExceedingLimit() throws Exception {
        fileIterator.limit = 2;
        fileIterator.curLimit = 3; 
        assertFalse(fileIterator.hasNext());
    }

    @Test
    void testNext() throws Exception {
        fileIterator.offset = 1;
        fileIterator.limit = 2;
        Mockito.doAnswer(invocation -> {
            if (fileIterator.curOffset < 5) {
                fileIterator.nextRow = new Object[] { "1", "2.0", "true" };
                fileIterator.curOffset++;
            } else {
                fileIterator.nextRow = null;
            }
            return null;
        }).when(fileIterator).getNextRow();
        assertTrue(fileIterator.hasNext());
        assertNotNull(fileIterator.next());
        assertTrue(fileIterator.hasNext());
        assertNotNull(fileIterator.next());
        assertFalse(fileIterator.hasNext());
    }

    @Test
    void testCleanRow() throws Exception {
        Object[] row = { "1", "2.0", "true" };
        Object[] cleanRow = fileIterator.cleanRow(row, fileIterator.types, fileIterator.additionalTypes);
        assertEquals(1, cleanRow[0]);
        assertEquals(2.0, cleanRow[1]);
        assertEquals(true, cleanRow[2]);
    }

    @Test
    void testCleanRowWithDifferentTypes() throws Exception {
        fileIterator.types = new SemossDataType[] { SemossDataType.INT, SemossDataType.DOUBLE, SemossDataType.BOOLEAN, SemossDataType.DATE, SemossDataType.STRING };
        fileIterator.additionalTypes = new String[] { null, null, null, "yyyy-MM-dd", null };
        Object[] row = { "1", "2.0", "true", "2023-01-01", "text" };
        Object[] cleanRow = fileIterator.cleanRow(row, fileIterator.types, fileIterator.additionalTypes);
        assertEquals(1, cleanRow[0]);
        assertEquals(2.0, cleanRow[1]);
        assertEquals(true, cleanRow[2]);
        assertNotNull(cleanRow[3]);
        assertEquals("text", cleanRow[4]);
    }

    @Test
    void testCleanRowWithNullValues() throws Exception {
        fileIterator.types = new SemossDataType[] { SemossDataType.INT, SemossDataType.DOUBLE, SemossDataType.BOOLEAN, SemossDataType.DATE, SemossDataType.STRING };
        fileIterator.additionalTypes = new String[] { null, null, null, "yyyy-MM-dd", null };
        Object[] row = { "null", "null", "null", "null", "null" };
        Object[] cleanRow = fileIterator.cleanRow(row, fileIterator.types, fileIterator.additionalTypes);
        assertEquals(null, cleanRow[0]);
        assertEquals(null, cleanRow[1]);
        assertEquals(null, cleanRow[2]);
        assertEquals(null, cleanRow[3]);
        assertEquals("null", cleanRow[4]);
    }

    @Test
    void testCleanRowWithDateHandling() throws Exception {
        fileIterator.types = new SemossDataType[] { SemossDataType.DATE };
        fileIterator.additionalTypes = new String[] { "yyyy-MM-dd" };
        Object[] row = { "2023-01-01" };
        Object[] cleanRow = fileIterator.cleanRow(row, fileIterator.types, fileIterator.additionalTypes);
        assertNotNull(cleanRow[0]);
        assertTrue(cleanRow[0] instanceof SemossDate);
    }

    @Test
    void testDeleteFile() throws IOException, Exception {
        fileIterator.deleteFile();
        File file = new File(fileIterator.fileLocation);
        assertFalse(file.exists());
    }

    @Test
    void testGetFileLocation() throws IOException, Exception {
        String fileLocation = fileIterator.getFileLocation();
        assertNotNull(fileLocation);
        assertEquals("test.csv", fileLocation);
    }

    @Test
    void testGetTypes() throws Exception {
        assertArrayEquals(new SemossDataType[] { SemossDataType.INT, SemossDataType.DOUBLE, SemossDataType.BOOLEAN }, fileIterator.types);
    }

    @Test
    void testGetHeaders() throws Exception {
        assertArrayEquals(new String[] { "col1", "col2", "col3" }, fileIterator.headers);
    }

    @Test
    void testGetNumRows() throws Exception {
        fileIterator.numRecords = 9;
        assertEquals(3, fileIterator.getNumRows());
    }

    @Test
    void testGetNumRecords() throws Exception {
        fileIterator.numRecords = 9;
        assertEquals(9, fileIterator.getNumRecords());
    }

    @Test
    void testHasNextWithEmptyFile() throws Exception {
        fileIterator.nextRow = null;
        assertFalse(fileIterator.hasNext());
    }

    @Test
    void testHasNextWithOnlyHeaders() throws Exception {
        fileIterator.nextRow = null;
        fileIterator.curOffset = 0;
        fileIterator.offset = 0;
        fileIterator.limit = 0;

        Mockito.doAnswer(invocation -> {
            fileIterator.nextRow = null;
            return null;
        }).when(fileIterator).getNextRow();

        assertFalse(fileIterator.hasNext());
    }

    @Test
    void testGetNumRecordsOverSize() throws Exception {
        fileIterator.headers = new String[] { "col1", "col2", "col3" };
        fileIterator.curOffset = 0;
        fileIterator.nextRow = new Object[] { "1", "2.0", "true" };

        Mockito.doAnswer(invocation -> {
            if (fileIterator.curOffset < 5) {
                fileIterator.nextRow = new Object[] { "1", "2.0", "true" };
                fileIterator.curOffset++;
            } else {
                fileIterator.nextRow = null;
            }
            return null;
        }).when(fileIterator).getNextRow();

        Mockito.doAnswer(invocation -> fileIterator.curOffset < 5).when(fileIterator).hasNext();

        long limit = 10;
        boolean result = fileIterator.getNumRecordsOverSize(limit);
        assertTrue(result);

        fileIterator.curOffset = 0;
        Mockito.doAnswer(invocation -> {
            if (fileIterator.curOffset < 2) {
                fileIterator.nextRow = new Object[] { "1", "2.0", "true" };
                fileIterator.curOffset++;
            } else {
                fileIterator.nextRow = null;
            }
            return null;
        }).when(fileIterator).getNextRow();

        Mockito.doAnswer(invocation -> fileIterator.curOffset < 2).when(fileIterator).hasNext();

        limit = 10;
        result = fileIterator.getNumRecordsOverSize(limit);
        assertFalse(result);
        assertEquals(6L, fileIterator.getNumRecords()); // 2 rows * 3 headers = 6
    }

    @Test
    void testSetAndGetOffsetsAndLimits() throws Exception {
        fileIterator.limit = 10;
        fileIterator.offset = 5;
        fileIterator.curOffset = 3;
        fileIterator.curLimit = 2;

        assertEquals(10, fileIterator.limit);
        assertEquals(5, fileIterator.offset);
        assertEquals(3, fileIterator.curOffset);
        assertEquals(2, fileIterator.curLimit);
    }

    @Test
    void testExecute() {
        fileIterator.execute();
    }

    @Test
    void testSetQuery() {
        fileIterator.setQuery("SELECT * FROM table");
    }

    @Test
    void testGetQuery() {
        assertNull(fileIterator.getQuery());
    }

    @Test
    void testSetEngine() {
        IDatabaseEngine engine = Mockito.mock(IDatabaseEngine.class);
        fileIterator.setEngine(engine);
    }

    @Test
    void testGetEngine() {
        assertNull(fileIterator.getEngine());
    }

    @Test
    void testFlushable() {
        assertFalse(fileIterator.flushable());
    }

    @Test
    void testFlush() {
        assertNull(fileIterator.flush());
    }
}
