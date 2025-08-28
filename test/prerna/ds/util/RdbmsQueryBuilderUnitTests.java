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
package prerna.ds.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import prerna.util.Utility;

public class RdbmsQueryBuilderUnitTests {

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void testEscapeForSQLStatement() {
    assertNull(RdbmsQueryBuilder.escapeForSQLStatement(null));
    assertEquals("test", RdbmsQueryBuilder.escapeForSQLStatement("test"));
    assertEquals("test''s", RdbmsQueryBuilder.escapeForSQLStatement("test's"));
  }

  @Test
  void testMakeMergeIntoQuery() {
    String leftTableName = "leftTable";
    String mergeTable = "mergeTable";
    String[] keyColumns = {"id"};
    String[] columnNames = {"name", "age"};

    String expectedQuery = "MERGE INTO leftTable KEY(id) (SELECT name,age FROM mergeTable)";
    String actualQuery =
        RdbmsQueryBuilder.makeMergeIntoQuery(leftTableName, mergeTable, keyColumns, columnNames);

    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  void testCreateTableFromFile() throws Exception {
    String fileName = "test.csv";
    Map<String, String> conceptTypes = new HashMap<>();
    conceptTypes.put("column1", "TYPE:VARCHAR");
    conceptTypes.put("column2", "TYPE:INTEGER");

    try (MockedStatic<Utility> mockedUtility = mockStatic(Utility.class);
        MockedStatic<Files> mockedFiles = mockStatic(Files.class);
        MockedStatic<Paths> mockedPaths = mockStatic(Paths.class)) {

      mockedUtility.when(() -> Utility.normalizePath(fileName)).thenReturn(fileName);
      mockedUtility.when(() -> Utility.getInstanceName("column1")).thenReturn("column1");
      mockedUtility.when(() -> Utility.getInstanceName("column2")).thenReturn("column2");
      mockedUtility.when(() -> Utility.getClassName("column1")).thenReturn("column1");
      mockedUtility.when(() -> Utility.getClassName("column2")).thenReturn("column2");

      Path mockPathMvDb = mock(Path.class);
      Path mockPathTraceDb = mock(Path.class);
      mockedPaths.when(() -> Paths.get("test.mv.db")).thenReturn(mockPathMvDb);
      mockedPaths.when(() -> Paths.get("test.trace.db")).thenReturn(mockPathTraceDb);

      mockedFiles.when(() -> Files.exists(mockPathMvDb)).thenReturn(true);
      mockedFiles.when(() -> Files.exists(mockPathTraceDb)).thenReturn(true);
      // mockedFiles.when(() -> Files.delete(mockPathMvDb));
      // mockedFiles.when(() -> Files.delete(mockPathTraceDb)).thenReturn(null);

      String expectedQuery =
          "DROP TABLE IF EXISTS column1; CREATE TABLE column1 (column1 VARCHAR, column2 INTEGER) AS SELECT column1, CONVERT(column2, Int) from CSVREAD('test.csv');";
      String actualQuery = RdbmsQueryBuilder.createTableFromFile(fileName, conceptTypes);

      assertEquals(expectedQuery, actualQuery);
    }
  }

  @Test
  void testCreateTableFromFileWithIOException() throws Exception {
    String fileName = "test.csv";
    Map<String, String> conceptTypes = new HashMap<>();
    conceptTypes.put("column1", "TYPE:VARCHAR");
    conceptTypes.put("column2", "TYPE:INTEGER");

    try (MockedStatic<Utility> mockedUtility = mockStatic(Utility.class);
        MockedStatic<Files> mockedFiles = mockStatic(Files.class);
        MockedStatic<Paths> mockedPaths = mockStatic(Paths.class)) {

      mockedUtility.when(() -> Utility.normalizePath(fileName)).thenReturn(fileName);
      mockedUtility.when(() -> Utility.getInstanceName("column1")).thenReturn("column1");
      mockedUtility.when(() -> Utility.getInstanceName("column2")).thenReturn("column2");
      mockedUtility.when(() -> Utility.getClassName("column1")).thenReturn("column1");
      mockedUtility.when(() -> Utility.getClassName("column2")).thenReturn("column2");

      Path mockPathMvDb = mock(Path.class);
      Path mockPathTraceDb = mock(Path.class);
      mockedPaths.when(() -> Paths.get("test.mv.db")).thenReturn(mockPathMvDb);
      mockedPaths.when(() -> Paths.get("test.trace.db")).thenReturn(mockPathTraceDb);

      mockedFiles.when(() -> Files.exists(mockPathMvDb)).thenReturn(true);
      mockedFiles.when(() -> Files.exists(mockPathTraceDb)).thenReturn(true);
      mockedFiles
          .when(() -> Files.delete(mockPathMvDb))
          .thenThrow(new IOException("Test Exception"));
      mockedFiles
          .when(() -> Files.delete(mockPathTraceDb))
          .thenThrow(new IOException("Test Exception"));

      String expectedQuery =
          "DROP TABLE IF EXISTS column1; CREATE TABLE column1 (column1 VARCHAR, column2 INTEGER) AS SELECT column1, CONVERT(column2, Int) from CSVREAD('test.csv');";
      String actualQuery = RdbmsQueryBuilder.createTableFromFile(fileName, conceptTypes);

      assertEquals(expectedQuery, actualQuery);
    }
  }

  @Test
  void testCreateTableFromFileWithNoFileDeletion() throws Exception {
    String fileName = "test.csv";
    Map<String, String> conceptTypes = new HashMap<>();
    conceptTypes.put("column1", "TYPE:VARCHAR");
    conceptTypes.put("column2", "TYPE:INTEGER");

    try (MockedStatic<Utility> mockedUtility = mockStatic(Utility.class);
        MockedStatic<Files> mockedFiles = mockStatic(Files.class);
        MockedStatic<Paths> mockedPaths = mockStatic(Paths.class)) {

      mockedUtility.when(() -> Utility.normalizePath(fileName)).thenReturn(fileName);
      mockedUtility.when(() -> Utility.getInstanceName("column1")).thenReturn("column1");
      mockedUtility.when(() -> Utility.getInstanceName("column2")).thenReturn("column2");
      mockedUtility.when(() -> Utility.getClassName("column1")).thenReturn("column1");
      mockedUtility.when(() -> Utility.getClassName("column2")).thenReturn("column2");

      Path mockPathMvDb = mock(Path.class);
      Path mockPathTraceDb = mock(Path.class);
      mockedPaths.when(() -> Paths.get("test.mv.db")).thenReturn(mockPathMvDb);
      mockedPaths.when(() -> Paths.get("test.trace.db")).thenReturn(mockPathTraceDb);

      mockedFiles.when(() -> Files.exists(mockPathMvDb)).thenReturn(false);
      mockedFiles.when(() -> Files.exists(mockPathTraceDb)).thenReturn(false);

      String expectedQuery =
          "DROP TABLE IF EXISTS column1; CREATE TABLE column1 (column1 VARCHAR, column2 INTEGER) AS SELECT column1, CONVERT(column2, Int) from CSVREAD('test.csv');";
      String actualQuery = RdbmsQueryBuilder.createTableFromFile(fileName, conceptTypes);

      assertEquals(expectedQuery, actualQuery);
      mockedFiles.verify(() -> Files.delete(mockPathMvDb), never());
      mockedFiles.verify(() -> Files.delete(mockPathTraceDb), never());
    }
  }

  @Test
  void testCreateTableFromFileWithUniqueRowId() throws Exception {
    String fileName = "test.csv";
    Map<String, String> conceptTypes = new HashMap<>();
    conceptTypes.put("UNIQUE_ROW_ID", "TYPE:INTEGER");

    try (MockedStatic<Utility> mockedUtility = mockStatic(Utility.class);
        MockedStatic<Files> mockedFiles = mockStatic(Files.class);
        MockedStatic<Paths> mockedPaths = mockStatic(Paths.class)) {

      mockedUtility.when(() -> Utility.normalizePath(fileName)).thenReturn(fileName);
      mockedUtility
          .when(() -> Utility.getInstanceName("UNIQUE_ROW_ID"))
          .thenReturn("UNIQUE_ROW_ID");
      mockedUtility.when(() -> Utility.getClassName("UNIQUE_ROW_ID")).thenReturn("UNIQUE_ROW_ID");

      Path mockPathMvDb = mock(Path.class);
      Path mockPathTraceDb = mock(Path.class);
      mockedPaths.when(() -> Paths.get("test.mv.db")).thenReturn(mockPathMvDb);
      mockedPaths.when(() -> Paths.get("test.trace.db")).thenReturn(mockPathTraceDb);

      mockedFiles.when(() -> Files.exists(mockPathMvDb)).thenReturn(false);
      mockedFiles.when(() -> Files.exists(mockPathTraceDb)).thenReturn(false);

      String expectedQuery =
          "DROP TABLE IF EXISTS UNIQUE_ROW_ID; CREATE TABLE UNIQUE_ROW_ID (UNIQUE_ROW_ID INTEGER) AS SELECT ROWNUM() from CSVREAD('test.csv');";
      String actualQuery = RdbmsQueryBuilder.createTableFromFile(fileName, conceptTypes);

      assertEquals(expectedQuery, actualQuery);
    }
  }

  @Test
  void testCreateTableFromFileWithVariousTypes() throws Exception {
    String fileName = "test.csv";
    Map<String, String> conceptTypes = new HashMap<>();
    conceptTypes.put("column1", "TYPE:DOUBLE");
    conceptTypes.put("column2", "TYPE:FLOAT");
    conceptTypes.put("column3", "TYPE:NUMBER");
    conceptTypes.put("column4", "TYPE:INTEGER");
    conceptTypes.put("column5", "TYPE:DATE");
    conceptTypes.put("column6", "TYPE:BIGINT");
    conceptTypes.put("column7", "TYPE:LONG");
    conceptTypes.put("column8", "TYPE:BOOLEAN");
    conceptTypes.put("column9", "TYPE:VARCHAR");

    try (MockedStatic<Utility> mockedUtility = mockStatic(Utility.class);
        MockedStatic<Files> mockedFiles = mockStatic(Files.class);
        MockedStatic<Paths> mockedPaths = mockStatic(Paths.class)) {

      mockedUtility.when(() -> Utility.normalizePath(fileName)).thenReturn(fileName);
      mockedUtility
          .when(() -> Utility.getInstanceName(ArgumentMatchers.anyString()))
          .thenAnswer(invocation -> invocation.getArgument(0));
      mockedUtility
          .when(() -> Utility.getClassName(ArgumentMatchers.anyString()))
          .thenAnswer(invocation -> invocation.getArgument(0));

      Path mockPathMvDb = mock(Path.class);
      Path mockPathTraceDb = mock(Path.class);
      mockedPaths.when(() -> Paths.get("test.mv.db")).thenReturn(mockPathMvDb);
      mockedPaths.when(() -> Paths.get("test.trace.db")).thenReturn(mockPathTraceDb);

      mockedFiles.when(() -> Files.exists(mockPathMvDb)).thenReturn(false);
      mockedFiles.when(() -> Files.exists(mockPathTraceDb)).thenReturn(false);

      String expectedQuery =
          "DROP TABLE IF EXISTS column1; CREATE TABLE column1 (column1 DOUBLE, column5 DATE, column4 INTEGER, column3 NUMBER, column2 FLOAT, column9 VARCHAR, column8 BOOLEAN, column7 LONG, column6 BIGINT) AS SELECT CONVERT(column1, Double), CONVERT(column5, Date), CONVERT(column4, Int), CONVERT(column3, Double), CONVERT(column2, Double), column9, CONVERT(column8, boolean), CONVERT(column7, Bigint), CONVERT(column6, Bigint) from CSVREAD('test.csv');";
      String actualQuery = RdbmsQueryBuilder.createTableFromFile(fileName, conceptTypes);

      assertEquals(expectedQuery, actualQuery);
    }
  }
}
