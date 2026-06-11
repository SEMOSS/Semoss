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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;

class CsvFileIteratorUnitTests {

    private CsvQueryStruct mockQueryStruct;
    private CsvFileIterator fileIterator;
    private File tempFile;

    @BeforeEach
    void setUp() throws IOException {
        tempFile = File.createTempFile("test", ".csv");
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("col1,col2,col3\n");
            writer.write("1,2,3\n");
            writer.write("4,5,6\n");
            writer.write("7,8,9\n");
        }

        mockQueryStruct = mock(CsvQueryStruct.class);
        when(mockQueryStruct.getFilePath()).thenReturn(tempFile.getAbsolutePath());
        when(mockQueryStruct.getDelimiter()).thenReturn(',');
        when(mockQueryStruct.getColumnTypes()).thenReturn(new HashMap<>());
        when(mockQueryStruct.getNewHeaderNames()).thenReturn(null);
        when(mockQueryStruct.getSelectors()).thenReturn(Arrays.asList());
        when(mockQueryStruct.getLimit()).thenReturn(0L);
        when(mockQueryStruct.getOffset()).thenReturn(0L);

        fileIterator = new CsvFileIterator(mockQueryStruct);
    }

    @AfterEach
    void tearDown() {
        if (tempFile.exists()) {
            tempFile.delete();
        }
    }

    @Test
    void testConstructor() {
        assertNotNull(fileIterator.getHelper());
        assertEquals(tempFile.getAbsolutePath(), fileIterator.getQs().getFilePath());
    }

    @Test
    void testReset() {
        fileIterator.reset();
        assertNotNull(fileIterator.getHelper());
    }

    @Test
    void testClose() throws IOException {
        fileIterator.close();
    }

    @Test
    void testConstructorWithDataTypeMap() throws IOException {
        Map<String, String> dataTypeMap = new HashMap<>();
        dataTypeMap.put("col1", "STRING");
        dataTypeMap.put("col2", "INTEGER");
        dataTypeMap.put("col3", "FLOAT");

        Map<String, String> additionalTypesMap = new HashMap<>();
        additionalTypesMap.put("col1", "ADDITIONAL_TYPE_1");
        additionalTypesMap.put("col2", "ADDITIONAL_TYPE_2");
        additionalTypesMap.put("col3", "ADDITIONAL_TYPE_3");

        when(mockQueryStruct.getColumnTypes()).thenReturn(dataTypeMap);
        when(mockQueryStruct.getAdditionalTypes()).thenReturn(additionalTypesMap);


        fileIterator = new CsvFileIterator(mockQueryStruct);

        assertNotNull(fileIterator.getHelper());
        assertEquals("STRING", fileIterator.getQs().getColumnTypes().get("col1"));
        assertEquals("INTEGER", fileIterator.getQs().getColumnTypes().get("col2"));
        assertEquals("FLOAT", fileIterator.getQs().getColumnTypes().get("col3"));

        assertEquals("ADDITIONAL_TYPE_1", fileIterator.getQs().getAdditionalTypes().get("col1"));
        assertEquals("ADDITIONAL_TYPE_2", fileIterator.getQs().getAdditionalTypes().get("col2"));
        assertEquals("ADDITIONAL_TYPE_3", fileIterator.getQs().getAdditionalTypes().get("col3"));

        assertArrayEquals(new String[]{"col1", "col2", "col3"}, fileIterator.getHelper().getHeaders());
    }
    
    @Test
    void testSetSelectorsWithInvalidSelectorType() {
        QueryColumnSelector invalidSelector = mock(QueryColumnSelector.class);
        when(invalidSelector.getSelectorType()).thenReturn(IQuerySelector.SELECTOR_TYPE.ARITHMETIC);

        when(mockQueryStruct.getSelectors()).thenReturn(Arrays.asList(invalidSelector));

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> {
            new CsvFileIterator(mockQueryStruct);
        });

        assertEquals("Cannot perform math on a csv import", thrown.getMessage());
    }
    


    @Test
    void testSetQs() {
        CsvQueryStruct newQueryStruct = mock(CsvQueryStruct.class);
        fileIterator.setQs(newQueryStruct);
        assertEquals(newQueryStruct, fileIterator.getQs());
    }
}
