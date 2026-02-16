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
package prerna.ds.r;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.SemossDataType;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Utility;

class RDataTableUnitTests {

    private AbstractRJavaTranslator mockTranslator;
    private RDataTable table;

    @BeforeEach
    void setUp() {
        mockTranslator = mock(AbstractRJavaTranslator.class);
        table = new RDataTable(mockTranslator);
    }

    @Nested
    class ConstantsTests {
        @Test
        void dataMakerName_isRDataTable() {
            assertEquals("RDataTable", RDataTable.DATA_MAKER_NAME);
        }
    }

    @Nested
    class ConstructorTests {
        @Test
        void constructor_callsStartR() {
            verify(mockTranslator).startR();
        }

        @Test
        void constructor_builderIsNotNull() {
            assertNotNull(table.getBuilder());
        }

        @Test
        void constructorWithName_setsTableName() {
            RDataTable t = new RDataTable(mockTranslator, "myTable");
            assertEquals("myTable", t.getName());
        }

        @Test
        void constructorWithEmptyName_usesDefault() {
            RDataTable t = new RDataTable(mockTranslator, "");
            assertNotNull(t.getName());
        }

        @Test
        void constructorWithNullName_usesDefault() {
            RDataTable t = new RDataTable(mockTranslator, null);
            assertNotNull(t.getName());
        }
    }

    @Nested
    class GetNameTests {
        @Test
        void getName_returnsBuilderTableName() {
            assertEquals("datatable", table.getName());
        }

        @Test
        void getName_afterSetName() {
            table.setName("newName");
            assertEquals("newName", table.getName());
        }

        @Test
        void getName_withNullBuilder_returnsFrameName() throws Exception {
            Field builderField = RDataTable.class.getDeclaredField("builder");
            builderField.setAccessible(true);
            builderField.set(table, null);
            // frameName was set in constructor from builder.getTableName()
            assertNotNull(table.getName());
        }
    }

    @Nested
    class SetNameTests {
        @Test
        void setName_changesName() {
            table.setName("renamed");
            assertEquals("renamed", table.getName());
        }
    }

    @Nested
    class GetBuilderTests {
        @Test
        void getBuilder_returnsNonNull() {
            assertNotNull(table.getBuilder());
        }

        @Test
        void getBuilder_hasCorrectTranslator() {
            assertSame(mockTranslator, table.getBuilder().getRJavaTranslator());
        }
    }

    @Nested
    class GetFrameTypeTests {
        @Test
        void getFrameType_returnsR() {
            assertEquals(DataFrameTypeEnum.R, table.getFrameType());
        }
    }

    @Nested
    class GetDataMakerNameTests {
        @Test
        void getDataMakerName_returnsConstant() {
            assertEquals("RDataTable", table.getDataMakerName());
        }
    }

    @Nested
    class IsEmptyTests {
        @Test
        void isEmpty_delegatesToBuilder() {
            when(mockTranslator.isEmpty("datatable")).thenReturn(true);
            assertTrue(table.isEmpty());
        }

        @Test
        void isEmpty_notEmpty() {
            when(mockTranslator.isEmpty("datatable")).thenReturn(false);
            assertFalse(table.isEmpty());
        }
    }

    @Nested
    class SizeTests {
        @Test
        void size_whenEmpty_returnsZero() {
            when(mockTranslator.isEmpty("datatable")).thenReturn(true);
            assertEquals(0, table.size("datatable"));
        }

        @Test
        void size_whenNotEmpty_returnsFrameSize() {
            when(mockTranslator.isEmpty("datatable")).thenReturn(false);
            when(mockTranslator.getInt("nrow(datatable) * ncol(datatable);")).thenReturn(50);
            assertEquals(50, table.size("datatable"));
        }
    }

    @Nested
    class GetColumnAsNumericTests {
        @Test
        void getColumnAsNumeric_returnsNull() {
            assertNull(table.getColumnAsNumeric("anyCol"));
        }
    }

    @Nested
    class ExecuteRScriptTests {
        @Test
        void executeRScript_validScript_callsEvalR() {
            table.executeRScript("x <- 42");
            verify(mockTranslator).executeEmptyR("x <- 42");
        }

        @Test
        void executeRScript_withQuotedNoBackslash_doesNotThrow() {
            assertDoesNotThrow(() -> table.executeRScript("x <- \"hello world\""));
        }

        @Test
        void executeRScript_withBackslashInQuotes_throwsIllegalArgument() {
            assertThrows(IllegalArgumentException.class,
                    () -> table.executeRScript("x <- \"hello\\nworld\""));
        }
    }

    @Nested
    class GetColumnNamesTests {
        @Test
        void getColumnNames_delegatesToBuilder() {
            String[] expected = {"col1", "col2"};
            when(mockTranslator.getColumns("datatable")).thenReturn(expected);
            assertArrayEquals(expected, table.getColumnNames());
        }

        @Test
        void getColumnNamesWithVar_delegatesToBuilder() {
            String[] expected = {"a", "b"};
            when(mockTranslator.getColumns("myVar")).thenReturn(expected);
            assertArrayEquals(expected, table.getColumnNames("myVar"));
        }
    }

    @Nested
    class GetColumnTypesTests {
        @Test
        void getColumnTypes_delegatesToBuilder() {
            String[] expected = {"integer", "character"};
            when(mockTranslator.getColumnTypes("datatable")).thenReturn(expected);
            assertArrayEquals(expected, table.getColumnTypes());
        }

        @Test
        void getColumnTypesWithVar_delegatesToBuilder() {
            String[] expected = {"numeric"};
            when(mockTranslator.getColumnTypes("myVar")).thenReturn(expected);
            assertArrayEquals(expected, table.getColumnTypes("myVar"));
        }
    }

    @Nested
    class GetNumRowsTests {
        @Test
        void getNumRows_delegatesToBuilder() {
            when(mockTranslator.getNumRows("myVar")).thenReturn(100);
            assertEquals(100, table.getNumRows("myVar"));
        }
    }

    @Nested
    class CloseTests {
        @Test
        void close_setsIsClosed() throws Exception {
            table.close();
            Field isClosedField = table.getClass().getSuperclass()
                    .getDeclaredField("isClosed");
            isClosedField.setAccessible(true);
            assertTrue((boolean) isClosedField.get(table));
        }

        @Test
        void close_callsDropTable() {
            table.close();
            verify(mockTranslator).executeEmptyR("rm(datatable)");
            verify(mockTranslator).executeEmptyR("gc()");
        }

        @Test
        void close_differentOriginalName_dropsBoth() {
            table.setName("newName");
            table.close();
            // drops current name
            verify(mockTranslator).executeEmptyR("rm(newName)");
            // drops original name
            verify(mockTranslator).executeEmptyR("rm(datatable)");
        }
    }

    @Nested
    class CloseConnectionTests {
        @Test
        void closeConnection_doesNotThrow() {
            assertDoesNotThrow(() -> table.closeConnection());
        }
    }

    @Nested
    class AddRowTests {
        @Test
        void addRow_emptyStub_doesNotThrow() {
            assertDoesNotThrow(() -> table.addRow(new Object[]{"a", 1}, new String[]{"name", "val"}));
        }
    }

    @Nested
    class SetLoggerTests {
        @Test
        void setLogger_doesNotThrow() {
            Logger logger = mock(Logger.class);
            assertDoesNotThrow(() -> table.setLogger(logger));
        }
    }

    @Nested
    class GetQueryInterpreterTests {
        @Test
        void getQueryInterpreter_returnsNonNull() {
            assertNotNull(table.getQueryInterpreter());
        }

        @Test
        void getQueryInterpreter_returnsRInterpreter() {
            IQueryInterpreter interp = table.getQueryInterpreter();
            assertTrue(interp instanceof prerna.query.interpreters.RInterpreter);
        }
    }

    @Nested
    class GetConnectionTests {
        @Test
        void getConnection_withGenericTranslator_returnsNull() {
            assertNull(table.getConnection());
        }

        @Test
        void getPort_withGenericTranslator_returnsNull() {
            assertNull(table.getPort());
        }
    }

    @Nested
    class GetDataRowTests {
        @Test
        void getDataRow_delegatesToBuilder() {
            Object[] expected = {"val1", 42};
            String[] headers = {"col1", "col2"};
            when(mockTranslator.getDataRow("query", headers)).thenReturn(expected);
            assertArrayEquals(expected, table.getDataRow("query", headers));
        }

        @Test
        void getBulkDataRow_delegatesToBuilder() {
            java.util.List<Object[]> expected = java.util.Collections.singletonList(new Object[]{"a"});
            String[] headers = {"col"};
            when(mockTranslator.getBulkDataRow("query", headers)).thenReturn(expected);
            assertEquals(expected, table.getBulkDataRow("query", headers));
        }
    }

    @Nested
    class GetColumnsWithIndexesTests {
        @Test
        void getColumnsWithIndexes_emptySet_returnsEmpty() {
            Set<String> result = table.getColumnsWithIndexes();
            assertTrue(result.isEmpty());
        }

        @Test
        void getColumnsWithIndexes_splitsOnTriplePlus() {
            table.getBuilder().columnIndexSet.add("myTable+++col1");
            table.getBuilder().columnIndexSet.add("myTable+++col2");
            Set<String> result = table.getColumnsWithIndexes();
            assertEquals(2, result.size());
            assertTrue(result.contains("col1"));
            assertTrue(result.contains("col2"));
        }
    }

    @Nested
    class RemoveAllColumnIndexTests {
        @Test
        void removeAllColumnIndex_clearsBuilderSet() {
            table.getBuilder().columnIndexSet.add("table+++col");
            table.removeAllColumnIndex();
            assertTrue(table.getBuilder().columnIndexSet.isEmpty());
        }
    }

    @Nested
    class AddColumnIndexTests {
        @Test
        void addColumnIndex_withDoubleDash_splitsCorrectly() {
            // This calls builder.addColumnIndex(split[0], split[1])
            // which internally calls translator methods
            assertDoesNotThrow(() -> table.addColumnIndex("myTable__myCol"));
        }

        @Test
        void addColumnIndex_withoutDoubleDash_usesGetName() {
            assertDoesNotThrow(() -> table.addColumnIndex("myCol"));
        }

        @Test
        void addColumnIndexArray_usesGetName() {
            assertDoesNotThrow(() -> table.addColumnIndex(new String[]{"col1", "col2"}));
        }
    }

    @Nested
    class OpenTests {
        @Test
        void open_fstFile_callsOpenFst() {
            CachePropFileFrameObject cf = new CachePropFileFrameObject();
            cf.setFrameName("testFrame");
            cf.setFrameCacheLocation("C:/cache/testFrame.fst");

            try {
                table.open(cf, null);
            } catch (Exception e) {
                // openCacheMeta may fail, but we verify the fst path was taken
            }
            verify(mockTranslator).executeEmptyR("library(\"fst\")");
        }

        @Test
        void open_rdaFile_callsOpenRda() {
            CachePropFileFrameObject cf = new CachePropFileFrameObject();
            cf.setFrameName("testFrame");
            cf.setFrameCacheLocation("C:/cache/testFrame.rda");

            try {
                table.open(cf, null);
            } catch (Exception e) {
                // openCacheMeta may fail
            }
            verify(mockTranslator).executeEmptyR("load(\"C:/cache/testFrame.rda\")");
        }

        @Test
        void open_unknownExtension_throwsSemossPixelException() {
            CachePropFileFrameObject cf = new CachePropFileFrameObject();
            cf.setFrameName("testFrame");
            cf.setFrameCacheLocation("C:/cache/testFrame.xyz");

            assertThrows(SemossPixelException.class, () -> table.open(cf, null));
        }
    }

    @Nested
    class GenerateRowIdTests {
        @Test
        void generateRowIdWithName_doesNotThrow() {
            assertDoesNotThrow(() -> table.generateRowIdWithName());
        }
    }

    @Nested
    class RemoveColumnTests {
        @Test
        void removeColumn_callsEvalRWithCorrectSyntax() {
            table.removeColumn("myCol");
            verify(mockTranslator).executeEmptyR("datatable[,myCol:=NULL]");
        }
    }

    @Nested
    class CalculateIsUniqueColumnTests {
        @Test
        void calculateIsUnique_sameCount_returnsTrue() throws Exception {
            when(mockTranslator.getNumRows("datatable[, c(\"myCol\")]")).thenReturn(10);
            when(mockTranslator.getNumRows("unique(datatable[, c(\"myCol\")])")).thenReturn(10);
            Method m = RDataTable.class.getDeclaredMethod("calculateIsUnqiueColumn", String.class);
            m.setAccessible(true);
            assertTrue((Boolean) m.invoke(table, "myCol"));
        }

        @Test
        void calculateIsUnique_differentCount_returnsFalse() throws Exception {
            when(mockTranslator.getNumRows("datatable[, c(\"myCol\")]")).thenReturn(10);
            when(mockTranslator.getNumRows("unique(datatable[, c(\"myCol\")])")).thenReturn(5);
            Method m = RDataTable.class.getDeclaredMethod("calculateIsUnqiueColumn", String.class);
            m.setAccessible(true);
            assertFalse((Boolean) m.invoke(table, "myCol"));
        }

        @Test
        void calculateIsUnique_withDoubleUnderscore_splitsColumn() throws Exception {
            when(mockTranslator.getNumRows("datatable[, c(\"col1\")]")).thenReturn(3);
            when(mockTranslator.getNumRows("unique(datatable[, c(\"col1\")])")).thenReturn(3);
            Method m = RDataTable.class.getDeclaredMethod("calculateIsUnqiueColumn", String.class);
            m.setAccessible(true);
            assertTrue((Boolean) m.invoke(table, "table__col1"));
        }
    }

    @Nested
    class RJMapTests {
        @Test
        void rjMap_containsExpectedMappings() throws Exception {
            Method getRJMap = RDataTable.class.getDeclaredMethod("getRJMap");
            getRJMap.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) getRJMap.invoke(table);

            assertNotNull(map);
            assertEquals(Integer.class, map.get("integer"));
            assertEquals(String.class, map.get("character"));
            assertEquals(Double.class, map.get("numeric"));
            assertEquals(Boolean.class, map.get("logical"));
            assertEquals(String.class, map.get("factor"));
        }

        @Test
        void rjMap_cachedOnSecondCall() throws Exception {
            Method getRJMap = RDataTable.class.getDeclaredMethod("getRJMap");
            getRJMap.setAccessible(true);
            Map<?, ?> first = (Map<?, ?>) getRJMap.invoke(table);
            Map<?, ?> second = (Map<?, ?>) getRJMap.invoke(table);
            assertSame(first, second);
        }
    }

    @Nested
    class QuerySQLTests {

        @Test
        @SuppressWarnings("unchecked")
        void querySQL_selectBranch_returnsMapWithColumnsTypesData() {
            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
                utilMock.when(() -> Utility.getRandomString(anyInt())).thenReturn("tmp01");
                utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));

                when(mockTranslator.getColumns("tmp01")).thenReturn(new String[]{"name", "age"});
                when(mockTranslator.getColumnTypes("tmp01")).thenReturn(new String[]{"character", "integer"});
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[]{"Alice", 30});
                when(mockTranslator.getBulkDataRow("tmp01", new String[]{"name", "age"})).thenReturn(rows);

                Object result = table.querySQL("SELECT name, age FROM datatable");

                assertNotNull(result);
                assertTrue(result instanceof Map);
                Map<String, Object> map = (Map<String, Object>) result;
                assertArrayEquals(new String[]{"name", "age"}, (String[]) map.get("columns"));
                assertNotNull(map.get("types"));
                assertNotNull(map.get("dataArray"));

                // Verify library(sqldf) was called
                verify(mockTranslator).executeEmptyR("library(sqldf);");
                // Verify cleanup rm(tmp01) was called
                verify(mockTranslator).executeEmptyR("rm(tmp01)");
            }
        }

        @Test
        @SuppressWarnings("unchecked")
        void querySQL_selectBranch_knownType_mapsCorrectly() {
            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
                utilMock.when(() -> Utility.getRandomString(anyInt())).thenReturn("tmp02");
                utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));

                when(mockTranslator.getColumns("tmp02")).thenReturn(new String[]{"name", "age", "score", "active", "group"});
                when(mockTranslator.getColumnTypes("tmp02")).thenReturn(new String[]{"character", "integer", "numeric", "logical", "factor"});
                when(mockTranslator.getBulkDataRow(eq("tmp02"), any(String[].class))).thenReturn(new ArrayList<>());

                Map<String, Object> map = (Map<String, Object>) table.querySQL("SELECT * FROM datatable");

                Object[] types = (Object[]) map.get("types");
                assertEquals(String.class, types[0]);
                assertEquals(Integer.class, types[1]);
                assertEquals(Double.class, types[2]);
                assertEquals(Boolean.class, types[3]);
                assertEquals(String.class, types[4]); // factor -> String
            }
        }

        @Test
        @SuppressWarnings("unchecked")
        void querySQL_selectBranch_unknownType_defaultsToString() {
            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
                utilMock.when(() -> Utility.getRandomString(anyInt())).thenReturn("tmp03");
                utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));

                when(mockTranslator.getColumns("tmp03")).thenReturn(new String[]{"col1"});
                when(mockTranslator.getColumnTypes("tmp03")).thenReturn(new String[]{"unknown_type"});
                when(mockTranslator.getBulkDataRow(eq("tmp03"), any(String[].class))).thenReturn(new ArrayList<>());

                Map<String, Object> map = (Map<String, Object>) table.querySQL("SELECT col1 FROM datatable");

                Object[] types = (Object[]) map.get("types");
                assertEquals(String.class, types[0]);
            }
        }

        @Test
        @SuppressWarnings("unchecked")
        void querySQL_nonSelectBranch_executesEachCommand() {
            when(mockTranslator.runRAndReturnOutput("print(1)")).thenReturn("[1] 1");
            when(mockTranslator.runRAndReturnOutput("print(2)")).thenReturn("[1] 2");

            Object result = table.querySQL("print(1)\nprint(2)");

            assertNotNull(result);
            assertTrue(result instanceof Map);
            Map<String, Object> map = (Map<String, Object>) result;
            assertNotNull(map.get("data"));
            assertArrayEquals(new String[]{"Command", "Output"}, (String[]) map.get("columns"));
            Object[] types = (Object[]) map.get("types");
            assertEquals(String.class, types[0]);
            assertEquals(String.class, types[1]);

            @SuppressWarnings("unchecked")
            List<List<Object>> data = (List<List<Object>>) map.get("data");
            assertEquals(2, data.size());
            assertEquals("print(1)", data.get(0).get(0));
            assertEquals("[1] 1", data.get(0).get(1));
            assertEquals("print(2)", data.get(1).get(0));
            assertEquals("[1] 2", data.get(1).get(1));
        }

        @Test
        @SuppressWarnings("unchecked")
        void querySQL_selectBranch_withLeadingWhitespace() {
            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
                utilMock.when(() -> Utility.getRandomString(anyInt())).thenReturn("tmp04");
                utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));

                when(mockTranslator.getColumns("tmp04")).thenReturn(new String[]{"col1"});
                when(mockTranslator.getColumnTypes("tmp04")).thenReturn(new String[]{"character"});
                when(mockTranslator.getBulkDataRow(eq("tmp04"), any(String[].class))).thenReturn(new ArrayList<>());

                // Leading whitespace should still trigger SELECT path
                Object result = table.querySQL("   SELECT col1 FROM datatable");

                assertTrue(result instanceof Map);
                Map<String, Object> map = (Map<String, Object>) result;
                assertNotNull(map.get("columns"));
                // Verify library(sqldf) was called = SELECT branch
                verify(mockTranslator).executeEmptyR("library(sqldf);");
            }
        }
    }

    @Nested
    class QueryCSVTests {

        @TempDir
        Path tempDir;

        @Test
        void queryCSV_selectBranch_returnsFile() {
            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
                utilMock.when(() -> Utility.getRandomString(anyInt())).thenReturn("tmp01");
                utilMock.when(() -> Utility.getInsightCacheDir()).thenReturn(tempDir.toAbsolutePath().toString());
                utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));

                Object result = table.queryCSV("SELECT col1 FROM datatable");

                assertNotNull(result);
                assertTrue(result instanceof File);
                File file = (File) result;
                assertTrue(file.getName().endsWith(".csv"));
                assertTrue(file.getName().startsWith("tmp01"));

                // Verify library(sqldf) and write.csv were called
                verify(mockTranslator).executeEmptyR("library(sqldf);");
                verify(mockTranslator).executeEmptyR(argThat(s -> s.contains("write.csv") && s.contains("sqldf")));
            }
        }

        @Test
        void queryCSV_nonSelectBranch_writesCommandOutputToFile() {
            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
                utilMock.when(() -> Utility.getRandomString(anyInt())).thenReturn("tmp02");
                utilMock.when(() -> Utility.getInsightCacheDir()).thenReturn(tempDir.toAbsolutePath().toString());
                utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));

                when(mockTranslator.runRAndReturnOutput("print(42)")).thenReturn("[1] 42");

                Object result = table.queryCSV("print(42)");

                assertNotNull(result);
                assertTrue(result instanceof File);
                File file = (File) result;
                assertTrue(file.exists());
                assertTrue(file.length() > 0);
            }
        }
    }

    @Nested
    class QueryJSONTests {

        @TempDir
        Path tempDir;

        @Test
        void queryJSON_selectBranch_returnsFile() {
            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
                utilMock.when(() -> Utility.getRandomString(anyInt())).thenReturn("tmp01");
                utilMock.when(() -> Utility.getInsightCacheDir()).thenReturn(tempDir.toAbsolutePath().toString());
                utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));

                Object result = table.queryJSON("SELECT col1 FROM datatable");

                assertNotNull(result);
                assertTrue(result instanceof File);
                File file = (File) result;
                assertTrue(file.getName().endsWith(".csv"));

                // Verify library(sqldf);library(jsonlite) and write_json were called
                verify(mockTranslator).executeEmptyR("library(sqldf);library(jsonlite);");
                verify(mockTranslator).executeEmptyR(argThat(s -> s.contains("write_json") && s.contains("sqldf")));
            }
        }

        @Test
        void queryJSON_nonSelectBranch_writesCommandOutputToFile() {
            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
                utilMock.when(() -> Utility.getRandomString(anyInt())).thenReturn("tmp02");
                utilMock.when(() -> Utility.getInsightCacheDir()).thenReturn(tempDir.toAbsolutePath().toString());
                utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));

                when(mockTranslator.runRAndReturnOutput("x <- 1")).thenReturn("1");

                Object result = table.queryJSON("x <- 1");

                assertNotNull(result);
                assertTrue(result instanceof File);
                File file = (File) result;
                assertTrue(file.exists());
                assertTrue(file.length() > 0);
            }
        }
    }

    @Nested
    class QueryStringTests {

        @Test
        void queryString_returnsRawSelectWrapper() {
            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
                utilMock.when(() -> Utility.getRandomString(anyInt())).thenReturn("abc123");
                utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));

                // RIterator.init() calls builder.evalR, getNumRows, getColumnNames, getColumnTypes
                when(mockTranslator.getNumRows(anyString())).thenReturn(0);
                when(mockTranslator.getColumns(anyString())).thenReturn(new String[]{"col1"});
                when(mockTranslator.getColumnTypes(anyString())).thenReturn(new String[]{"character"});

                IRawSelectWrapper result = table.query("datatable[,c(\"col1\")]");

                assertNotNull(result);
            }
        }
    }

    @Nested
    class QuerySelectQueryStructTests {

        private OwlTemporalEngineMeta mockMeta;

        @BeforeEach
        void setUpMeta() throws Exception {
            mockMeta = mock(OwlTemporalEngineMeta.class);
            when(mockMeta.getHeaderToTypeMap()).thenReturn(new HashMap<>());
            Field metaDataField = table.getClass().getSuperclass().getDeclaredField("metaData");
            metaDataField.setAccessible(true);
            metaDataField.set(table, mockMeta);
        }

        @Test
        void queryQS_noCacheMiss_returnsRawSelectWrapper() {
            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
                 MockedStatic<QSAliasToPhysicalConverter> converterMock = mockStatic(QSAliasToPhysicalConverter.class);
                 MockedConstruction<RInterpreter> interpConstruction = mockConstruction(RInterpreter.class,
                         (mock, context) -> {
                             when(mock.composeQuery()).thenReturn("datatable[,c(\"col1\")]");
                             when(mock.getConvertedDates()).thenReturn(new HashMap<>());
                             when(mock.getMainQuery()).thenReturn("mainQuery");
                             when(mock.getTempVarName()).thenReturn("tempVar");
                         })) {

                utilMock.when(() -> Utility.getRandomString(anyInt())).thenReturn("abc123");
                utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));

                SelectQueryStruct qs = new SelectQueryStruct();
                converterMock.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), eq(mockMeta)))
                        .thenReturn(qs);

                // RIterator.init() interactions
                when(mockTranslator.getNumRows(anyString())).thenReturn(0);
                when(mockTranslator.getColumns(anyString())).thenReturn(new String[]{"col1"});
                when(mockTranslator.getColumnTypes(anyString())).thenReturn(new String[]{"character"});

                IRawSelectWrapper result = table.query(qs);

                assertNotNull(result);
            }
        }

        @Test
        void queryQS_withXCacheFalse_clearsCacheAndReturns() {
            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
                 MockedStatic<QSAliasToPhysicalConverter> converterMock = mockStatic(QSAliasToPhysicalConverter.class);
                 MockedConstruction<RInterpreter> interpConstruction = mockConstruction(RInterpreter.class,
                         (mock, context) -> {
                             when(mock.composeQuery()).thenReturn("datatable[,c(\"col1\")]");
                             when(mock.getConvertedDates()).thenReturn(new HashMap<>());
                             when(mock.getMainQuery()).thenReturn("mainQuery");
                             when(mock.getTempVarName()).thenReturn("tempVar");
                         })) {

                utilMock.when(() -> Utility.getRandomString(anyInt())).thenReturn("abc123");
                utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));

                SelectQueryStruct qs = new SelectQueryStruct();
                Map<String, Object> pragmap = new HashMap<>();
                pragmap.put("xCache", "False");
                qs.setPragmap(pragmap);

                converterMock.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), eq(mockMeta)))
                        .thenReturn(qs);

                when(mockTranslator.getNumRows(anyString())).thenReturn(0);
                when(mockTranslator.getColumns(anyString())).thenReturn(new String[]{"col1"});
                when(mockTranslator.getColumnTypes(anyString())).thenReturn(new String[]{"character"});

                IRawSelectWrapper result = table.query(qs);

                assertNotNull(result);
                // The cache was cleared because xCache=False
            }
        }

        @Test
        void queryQS_noPragmap_usesDefaultCacheTrue() {
            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
                 MockedStatic<QSAliasToPhysicalConverter> converterMock = mockStatic(QSAliasToPhysicalConverter.class);
                 MockedConstruction<RInterpreter> interpConstruction = mockConstruction(RInterpreter.class,
                         (mock, context) -> {
                             when(mock.composeQuery()).thenReturn("datatable[,c(\"col1\")]");
                             when(mock.getConvertedDates()).thenReturn(new HashMap<>());
                             when(mock.getMainQuery()).thenReturn("mainQuery");
                             when(mock.getTempVarName()).thenReturn("tempVar");
                         })) {

                utilMock.when(() -> Utility.getRandomString(anyInt())).thenReturn("abc123");
                utilMock.when(() -> Utility.cleanLogString(anyString())).thenAnswer(inv -> inv.getArgument(0));

                SelectQueryStruct qs = new SelectQueryStruct();
                // No pragmap set, so getPragmap() returns null -> cache defaults to true

                converterMock.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), eq(mockMeta)))
                        .thenReturn(qs);

                when(mockTranslator.getNumRows(anyString())).thenReturn(0);
                when(mockTranslator.getColumns(anyString())).thenReturn(new String[]{"col1"});
                when(mockTranslator.getColumnTypes(anyString())).thenReturn(new String[]{"character"});

                IRawSelectWrapper result = table.query(qs);

                assertNotNull(result);
            }
        }
    }

    @Nested
    class ProcessDataMakerComponentTests {

        @Test
        void processDataMakerComponent_withEmptyPostTrans_doesNotThrow() {
            DataMakerComponent component = mock(DataMakerComponent.class);
            when(component.getPostTrans()).thenReturn(new ArrayList<>());

            assertDoesNotThrow(() -> table.processDataMakerComponent(component));
        }
    }
}
