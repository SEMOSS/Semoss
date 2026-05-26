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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.algorithm.api.SemossDataType;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.reactor.frame.r.util.RJavaRserveTranslator;
import prerna.reactor.frame.r.util.RJavaUserRserveTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

class RFrameBuilderUnitTests {

    private AbstractRJavaTranslator mockTranslator;
    private RFrameBuilder builder;

    @BeforeEach
    void setUp() {
        mockTranslator = mock(AbstractRJavaTranslator.class);
        builder = new RFrameBuilder(mockTranslator);
    }

    @Nested
    class ConstantsTests {
        @Test
        void className_isCorrect() {
            assertEquals("prerna.ds.r.RFrameBuilder", RFrameBuilder.CLASS_NAME);
        }
    }

    @Nested
    class ConstructorTests {
        @Test
        void constructor_callsStartR() {
            verify(mockTranslator).startR();
        }

        @Test
        void constructor_defaultTableName() {
            assertEquals("datatable", builder.getTableName());
        }

        @Test
        void constructorWithName_setsTableName() {
            RFrameBuilder b = new RFrameBuilder(mockTranslator, "myTable");
            assertEquals("myTable", b.getTableName());
        }

        @Test
        void constructorWithName_callsStartR() {
            AbstractRJavaTranslator t = mock(AbstractRJavaTranslator.class);
            new RFrameBuilder(t, "test");
            verify(t).startR();
        }
    }

    @Nested
    class GetSetTests {
        @Test
        void setTableName_changesName() {
            builder.setTableName("newTable");
            assertEquals("newTable", builder.getTableName());
        }

        @Test
        void getRJavaTranslator_returnsMock() {
            assertSame(mockTranslator, builder.getRJavaTranslator());
        }

        @Test
        void setLogger_doesNotThrow() {
            Logger logger = mock(Logger.class);
            assertDoesNotThrow(() -> builder.setLogger(logger));
        }

        @Test
        void isInMem_defaultTrue() {
            assertTrue(builder.isInMem);
        }
    }

    @Nested
    class EvalRTests {
        @Test
        void evalR_delegatesToTranslator() {
            builder.evalR("x <- 1");
            verify(mockTranslator).executeEmptyR("x <- 1");
        }
    }

    @Nested
    class IsEmptyTests {
        @Test
        void isEmpty_delegatesWithDefaultTableName() {
            when(mockTranslator.isEmpty("datatable")).thenReturn(true);
            assertTrue(builder.isEmpty());
        }

        @Test
        void isEmpty_notEmpty() {
            when(mockTranslator.isEmpty("datatable")).thenReturn(false);
            assertFalse(builder.isEmpty());
        }

        @Test
        void isEmptyWithName_delegatesWithFrameName() {
            when(mockTranslator.isEmpty("myFrame")).thenReturn(true);
            assertTrue(builder.isEmpty("myFrame"));
        }
    }

    @Nested
    class GetNumRowsTests {
        @Test
        void getNumRows_delegatesWithDefaultTableName() {
            when(mockTranslator.getNumRows("datatable")).thenReturn(10);
            assertEquals(10, builder.getNumRows());
        }

        @Test
        void getNumRowsWithName_delegatesWithVarName() {
            when(mockTranslator.getNumRows("myVar")).thenReturn(42);
            assertEquals(42, builder.getNumRows("myVar"));
        }
    }

    @Nested
    class GetFrameSizeTests {
        @Test
        void getFrameSize_callsGetIntWithCorrectSyntax() {
            when(mockTranslator.getInt("nrow(myFrame) * ncol(myFrame);")).thenReturn(100);
            assertEquals(100, builder.getFrameSize("myFrame"));
        }
    }

    @Nested
    class ColumnNameTypeTests {
        @Test
        void getColumnNames_delegatesWithDefaultTableName() {
            String[] expected = {"col1", "col2"};
            when(mockTranslator.getColumns("datatable")).thenReturn(expected);
            assertArrayEquals(expected, builder.getColumnNames());
        }

        @Test
        void getColumnNamesWithVar_delegates() {
            String[] expected = {"a", "b", "c"};
            when(mockTranslator.getColumns("myVar")).thenReturn(expected);
            assertArrayEquals(expected, builder.getColumnNames("myVar"));
        }

        @Test
        void getColumnTypes_delegatesWithDefaultTableName() {
            String[] expected = {"integer", "character"};
            when(mockTranslator.getColumnTypes("datatable")).thenReturn(expected);
            assertArrayEquals(expected, builder.getColumnTypes());
        }

        @Test
        void getColumnTypesWithVar_delegates() {
            String[] expected = {"numeric", "logical"};
            when(mockTranslator.getColumnTypes("myVar")).thenReturn(expected);
            assertArrayEquals(expected, builder.getColumnTypes("myVar"));
        }
    }

    @Nested
    class DataRowTests {
        @Test
        void getDataRow_delegatesToTranslator() {
            Object[] expected = {"val1", 42};
            String[] headers = {"col1", "col2"};
            when(mockTranslator.getDataRow("query", headers)).thenReturn(expected);
            assertArrayEquals(expected, builder.getDataRow("query", headers));
        }

        @Test
        void getBulkDataRow_delegatesToTranslator() {
            List<Object[]> expected = java.util.Collections.singletonList(new Object[]{"a", 1});
            String[] headers = {"name", "value"};
            when(mockTranslator.getBulkDataRow("query", headers)).thenReturn(expected);
            assertEquals(expected, builder.getBulkDataRow("query", headers));
        }
    }

    @Nested
    class GetConnectionTests {
        @Test
        void getConnection_withGenericTranslator_returnsNull() {
            assertNull(builder.getConnection());
        }

        @Test
        void getPort_withGenericTranslator_returnsNull() {
            assertNull(builder.getPort());
        }
    }

    @Nested
    class DropTableTests {
        @Test
        void dropTable_callsRmAndGc() {
            builder.dropTable();
            verify(mockTranslator).executeEmptyR("rm(datatable)");
            verify(mockTranslator).executeEmptyR("gc()");
        }

        @Test
        void dropTable_usesCurrentTableName() {
            builder.setTableName("customTable");
            builder.dropTable();
            verify(mockTranslator).executeEmptyR("rm(customTable)");
            verify(mockTranslator).executeEmptyR("gc()");
        }
    }

    @Nested
    class ColumnIndexTests {
        @Test
        void removeAllColumnIndex_clearsSet() {
            builder.columnIndexSet.add("table+++col1");
            builder.columnIndexSet.add("table+++col2");
            builder.removeAllColumnIndex();
            assertTrue(builder.columnIndexSet.isEmpty());
        }

        @Test
        void columnIndexSet_initiallyEmpty() {
            assertTrue(builder.columnIndexSet.isEmpty());
        }
    }

    @Nested
    class AddColumnIndexSingleTests {
        @Test
        void addColumnIndex_newIndex_callsSetindex() {
            builder.addColumnIndex("myTable", "col1");
            verify(mockTranslator).executeEmptyR("setindex(myTable,col1);");
        }

        @Test
        void addColumnIndex_newIndex_callsGetIndices() {
            builder.addColumnIndex("myTable", "col1");
            verify(mockTranslator).getStringArray("indices(myTable);");
        }

        @Test
        void addColumnIndex_confirmedIndex_addsToSet() {
            when(mockTranslator.getStringArray("indices(myTable);"))
                    .thenReturn(new String[]{"col1"});
            builder.addColumnIndex("myTable", "col1");
            assertTrue(builder.columnIndexSet.contains("myTable+++col1"));
        }

        @Test
        void addColumnIndex_notConfirmed_doesNotAddToSet() {
            when(mockTranslator.getStringArray("indices(myTable);"))
                    .thenReturn(new String[]{"otherCol"});
            builder.addColumnIndex("myTable", "col1");
            assertFalse(builder.columnIndexSet.contains("myTable+++col1"));
        }

        @Test
        void addColumnIndex_nullIndices_doesNotAddToSet() {
            when(mockTranslator.getStringArray("indices(myTable);"))
                    .thenReturn(null);
            builder.addColumnIndex("myTable", "col1");
            assertFalse(builder.columnIndexSet.contains("myTable+++col1"));
        }

        @Test
        void addColumnIndex_alreadyExists_skipsExecution() {
            builder.columnIndexSet.add("myTable+++col1");
            builder.addColumnIndex("myTable", "col1");
            verify(mockTranslator, never()).executeEmptyR("setindex(myTable,col1);");
        }

        @Test
        void addColumnIndex_exceptionInTranslator_doesNotThrow() {
            when(mockTranslator.getStringArray("indices(myTable);"))
                    .thenThrow(new RuntimeException("R error"));
            // Exception is caught inside addColumnIndex
            assertDoesNotThrow(() -> builder.addColumnIndex("myTable", "col1"));
        }
    }

    @Nested
    class AddColumnIndexArrayTests {
        @Test
        void addColumnIndex_array_callsLapplyAndGetIndices() {
            builder.addColumnIndex("myTable", new String[]{"col1"});
            verify(mockTranslator).executeEmptyR(
                    "invisible(lapply(c('col1'), setindexv, x= myTable));");
            verify(mockTranslator).getStringArray("indices(myTable);");
        }

        @Test
        void addColumnIndex_array_confirmedIndices_addsToSet() {
            when(mockTranslator.getStringArray("indices(myTable);"))
                    .thenReturn(new String[]{"col1", "col2"});
            builder.addColumnIndex("myTable", new String[]{"col1", "col2"});
            assertTrue(builder.columnIndexSet.contains("myTable+++col1"));
            assertTrue(builder.columnIndexSet.contains("myTable+++col2"));
        }

        @Test
        void addColumnIndex_array_partialConfirm_addsOnlyConfirmed() {
            when(mockTranslator.getStringArray("indices(myTable);"))
                    .thenReturn(new String[]{"col1"});
            builder.addColumnIndex("myTable", new String[]{"col1", "col2"});
            assertTrue(builder.columnIndexSet.contains("myTable+++col1"));
            assertFalse(builder.columnIndexSet.contains("myTable+++col2"));
        }

        @Test
        void addColumnIndex_array_nullIndices_doesNotAddToSet() {
            when(mockTranslator.getStringArray("indices(myTable);"))
                    .thenReturn(null);
            builder.addColumnIndex("myTable", new String[]{"col1"});
            assertFalse(builder.columnIndexSet.contains("myTable+++col1"));
        }

        @Test
        void addColumnIndex_array_allAlreadyIndexed_skipsExecution() {
            builder.columnIndexSet.add("col1");
            builder.columnIndexSet.add("col2");
            builder.addColumnIndex("myTable", new String[]{"col1", "col2"});
            verify(mockTranslator, never()).getStringArray("indices(myTable);");
        }

        @Test
        void addColumnIndex_array_exceptionInTranslator_doesNotThrow() {
            when(mockTranslator.getStringArray("indices(myTable);"))
                    .thenThrow(new RuntimeException("R error"));
            assertDoesNotThrow(() -> builder.addColumnIndex("myTable", new String[]{"col1"}));
        }
    }

    @Nested
    class OpenRdaTests {
        @Test
        void openRda_callsLoadWithCorrectSyntax() {
            builder.openRda("C:\\path\\to\\file.rda");
            verify(mockTranslator).executeEmptyR("load(\"C:/path/to/file.rda\")");
        }

        @Test
        void openRda_forwardSlashPath() {
            builder.openRda("/home/user/file.rda");
            verify(mockTranslator).executeEmptyR("load(\"/home/user/file.rda\")");
        }
    }

    @Nested
    class OpenFstTests {
        @Test
        void openFst_callsLibraryAndReadFst() {
            builder.openFst("C:\\path\\to\\file.fst", "myFrame");
            verify(mockTranslator).executeEmptyR("library(\"fst\")");
            verify(mockTranslator).executeEmptyR(
                    "myFrame <- as.data.table(read_fst(\"C:/path/to/file.fst\"))");
        }
    }

    // ---------------------------------------------------------------
    // alterColumnTypes (private, biggest coverage win)
    // ---------------------------------------------------------------
    @Nested
    class AlterColumnTypesTests {

        private Method alterColumnTypesMethod;

        @BeforeEach
        void setUpReflection() throws Exception {
            alterColumnTypesMethod = RFrameBuilder.class.getDeclaredMethod(
                    "alterColumnTypes", String.class, Map.class, Map.class, String.class);
            alterColumnTypesMethod.setAccessible(true);
        }

        private void invoke(String tableName, Map<String, SemossDataType> typesMap,
                            Map<String, String> javaDateFormatMap, String fileType) throws Exception {
            alterColumnTypesMethod.invoke(builder, tableName, typesMap, javaDateFormatMap, fileType);
        }

        @Test
        void stringColumns_callsCharacterAndReplaceNA() throws Exception {
            Map<String, SemossDataType> types = new HashMap<>();
            types.put("name", SemossDataType.STRING);

            invoke("tbl", types, new HashMap<>(), "csv");

            // evalR delegates to executeEmptyR; string columns trigger 2 calls:
            // alterColumnTypeToCharacter + replaceNAString
            verify(mockTranslator, times(2)).executeEmptyR(anyString());
        }

        @Test
        void intColumns_callsAlterToInteger() throws Exception {
            Map<String, SemossDataType> types = new HashMap<>();
            types.put("age", SemossDataType.INT);

            invoke("tbl", types, new HashMap<>(), "csv");

            verify(mockTranslator, times(1)).executeEmptyR(anyString());
        }

        @Test
        void doubleColumns_callsAlterToNumeric() throws Exception {
            Map<String, SemossDataType> types = new HashMap<>();
            types.put("score", SemossDataType.DOUBLE);

            invoke("tbl", types, new HashMap<>(), "csv");

            verify(mockTranslator, times(1)).executeEmptyR(anyString());
        }

        @Test
        void booleanColumns_callsAlterToBoolean() throws Exception {
            Map<String, SemossDataType> types = new HashMap<>();
            types.put("flag", SemossDataType.BOOLEAN);

            invoke("tbl", types, new HashMap<>(), "csv");

            verify(mockTranslator, times(1)).executeEmptyR(anyString());
        }

        @Test
        void dateColumns_defaultFormat() throws Exception {
            Map<String, SemossDataType> types = new HashMap<>();
            types.put("dob", SemossDataType.DATE);

            // null javaDateFormatMap -> uses "yyyy-MM-dd" default
            invoke("tbl", types, null, "csv");

            verify(mockTranslator, times(1)).runR(anyString());
        }

        @Test
        void dateColumns_customFormat() throws Exception {
            Map<String, SemossDataType> types = new HashMap<>();
            types.put("dob", SemossDataType.DATE);

            Map<String, String> dateFormats = new HashMap<>();
            dateFormats.put("dob", "MM/dd/yyyy");

            invoke("tbl", types, dateFormats, "csv");

            verify(mockTranslator, times(1)).runR(anyString());
        }

        @Test
        void dateColumns_sameFormat_grouped() throws Exception {
            Map<String, SemossDataType> types = new HashMap<>();
            types.put("startDate", SemossDataType.DATE);
            types.put("endDate", SemossDataType.DATE);

            Map<String, String> dateFormats = new HashMap<>();
            dateFormats.put("startDate", "yyyy-MM-dd");
            dateFormats.put("endDate", "yyyy-MM-dd");

            invoke("tbl", types, dateFormats, "csv");

            // Same format -> grouped into one runR call
            verify(mockTranslator, times(1)).runR(anyString());
        }

        @Test
        void timestampColumns_nonExcelNonEmpty() throws Exception {
            Map<String, SemossDataType> types = new HashMap<>();
            types.put("ts", SemossDataType.TIMESTAMP);

            when(mockTranslator.isEmpty("tbl")).thenReturn(false);

            invoke("tbl", types, null, "csv");

            // Should call runR for alterColumnTypeToDateTime
            verify(mockTranslator, times(1)).runR(anyString());
        }

        @Test
        void timestampColumns_nonExcelEmpty() throws Exception {
            Map<String, SemossDataType> types = new HashMap<>();
            types.put("ts", SemossDataType.TIMESTAMP);

            when(mockTranslator.isEmpty("tbl")).thenReturn(true);

            invoke("tbl", types, null, "csv");

            // Should call runR for alterEmptyTableColumnTypeToDateTime
            verify(mockTranslator, times(1)).runR(anyString());
        }

        @Test
        void timestampColumns_excelFileType_skipped() throws Exception {
            Map<String, SemossDataType> types = new HashMap<>();
            types.put("ts", SemossDataType.TIMESTAMP);

            invoke("tbl", types, null, "excel");

            // excel file type -> timestamp block skipped
            verify(mockTranslator, never()).runR(anyString());
        }

        @Test
        void mixedTypes_allProcessed() throws Exception {
            Map<String, SemossDataType> types = new HashMap<>();
            types.put("name", SemossDataType.STRING);
            types.put("age", SemossDataType.INT);
            types.put("score", SemossDataType.DOUBLE);

            invoke("tbl", types, new HashMap<>(), "csv");

            // STRING -> 2 evalR calls (character + replaceNA)
            // INT -> 1 evalR call
            // DOUBLE -> 1 evalR call
            // Total = 4
            verify(mockTranslator, times(4)).executeEmptyR(anyString());
        }
    }

    // ---------------------------------------------------------------
    // updateFilter (private)
    // ---------------------------------------------------------------
    @Nested
    class UpdateFilterTests {

        private Method updateFilterMethod;

        @BeforeEach
        void setUpReflection() throws Exception {
            updateFilterMethod = RFrameBuilder.class.getDeclaredMethod(
                    "updateFilter", String.class, SimpleQueryFilter.class);
            updateFilterMethod.setAccessible(true);
        }

        private SimpleQueryFilter invoke(String tableName, SimpleQueryFilter filter) throws Exception {
            return (SimpleQueryFilter) updateFilterMethod.invoke(builder, tableName, filter);
        }

        @Test
        void colToCol_prefixesBothSides() throws Exception {
            NounMetadata left = new NounMetadata("colA", PixelDataType.COLUMN);
            NounMetadata right = new NounMetadata("colB", PixelDataType.COLUMN);
            SimpleQueryFilter filter = new SimpleQueryFilter(left, "==", right);

            SimpleQueryFilter result = invoke("myTable", filter);

            assertNotNull(result);
            assertEquals("myTable__colA", result.getLComparison().getValue().toString());
            assertEquals("myTable__colB", result.getRComparison().getValue().toString());
        }

        @Test
        void colToValues_prefixesLeftSide() throws Exception {
            NounMetadata left = new NounMetadata("colA", PixelDataType.COLUMN);
            NounMetadata right = new NounMetadata("someValue", PixelDataType.CONST_STRING);
            SimpleQueryFilter filter = new SimpleQueryFilter(left, "==", right);

            SimpleQueryFilter result = invoke("myTable", filter);

            assertNotNull(result);
            assertEquals("myTable__colA", result.getLComparison().getValue().toString());
            assertEquals("someValue", result.getRComparison().getValue().toString());
        }

        @Test
        void valuesToCol_prefixesRightSide() throws Exception {
            NounMetadata left = new NounMetadata("someValue", PixelDataType.CONST_STRING);
            NounMetadata right = new NounMetadata("colB", PixelDataType.COLUMN);
            SimpleQueryFilter filter = new SimpleQueryFilter(left, "==", right);

            SimpleQueryFilter result = invoke("myTable", filter);

            assertNotNull(result);
            assertEquals("someValue", result.getLComparison().getValue().toString());
            assertEquals("myTable__colB", result.getRComparison().getValue().toString());
        }

        @Test
        void valueToValue_returnsNull() throws Exception {
            NounMetadata left = new NounMetadata("val1", PixelDataType.CONST_STRING);
            NounMetadata right = new NounMetadata("val2", PixelDataType.CONST_STRING);
            SimpleQueryFilter filter = new SimpleQueryFilter(left, "==", right);

            SimpleQueryFilter result = invoke("myTable", filter);

            assertNull(result);
        }
    }

    // ---------------------------------------------------------------
    // updateFileSelectors (private)
    // ---------------------------------------------------------------
    @Nested
    class UpdateFileSelectorsTests {

        private Method updateFileSelectorsMethod;

        @BeforeEach
        void setUpReflection() throws Exception {
            updateFileSelectorsMethod = RFrameBuilder.class.getDeclaredMethod(
                    "updateFileSelectors", SelectQueryStruct.class, String.class, String[].class);
            updateFileSelectorsMethod.setAccessible(true);
        }

        @Test
        void addsSelectorsForEachColumn() throws Exception {
            SelectQueryStruct qs = new SelectQueryStruct();
            String[] colNames = {"col1", "col2", "col3"};

            SelectQueryStruct result = (SelectQueryStruct) updateFileSelectorsMethod.invoke(
                    builder, qs, "tbl", colNames);

            assertEquals(3, result.getSelectors().size());
        }

        @Test
        void emptyArray_noSelectors() throws Exception {
            SelectQueryStruct qs = new SelectQueryStruct();
            String[] colNames = {};

            SelectQueryStruct result = (SelectQueryStruct) updateFileSelectorsMethod.invoke(
                    builder, qs, "tbl", colNames);

            assertEquals(0, result.getSelectors().size());
        }
    }

    // ---------------------------------------------------------------
    // saveRda (protected)
    // ---------------------------------------------------------------
    @Nested
    class SaveRdaTests {

        @TempDir
        Path tempDir;

        @Test
        void saveRda_callsEvalRWithCorrectSyntax() throws Exception {
            Path tempFile = tempDir.resolve("frame.rda");
            Files.writeString(tempFile, "content");
            String filePath = tempFile.toAbsolutePath().toString();

            builder.saveRda(filePath, "myFrame");

            verify(mockTranslator).executeEmptyR(
                    "save(myFrame, file=\"" + filePath.replace("\\", "/") + "\")");
        }

        @Test
        void saveRda_throwsWhenFileEmpty() throws Exception {
            Path tempFile = tempDir.resolve("empty.rda");
            Files.writeString(tempFile, "");
            String filePath = tempFile.toAbsolutePath().toString();

            assertThrows(IllegalArgumentException.class,
                    () -> builder.saveRda(filePath, "myFrame"));
        }

        @Test
        void saveRda_throwsWhenFileDoesNotExist() {
            String filePath = tempDir.resolve("nonexistent.rda").toAbsolutePath().toString();

            // File does not exist -> length() returns 0 -> throws
            assertThrows(IllegalArgumentException.class,
                    () -> builder.saveRda(filePath, "myFrame"));
        }
    }

    // ---------------------------------------------------------------
    // saveFst (protected)
    // ---------------------------------------------------------------
    @Nested
    class SaveFstTests {

        @TempDir
        Path tempDir;

        @Test
        void saveFst_callsLibraryAndWriteFst() throws Exception {
            Path tempFile = tempDir.resolve("frame.fst");
            Files.writeString(tempFile, "content");
            String filePath = tempFile.toAbsolutePath().toString();

            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
                utilMock.when(() -> Utility.normalizePath(anyString())).thenReturn(filePath);

                builder.saveFst(filePath, "myFrame");

                verify(mockTranslator).executeEmptyR("library(\"fst\")");
                verify(mockTranslator).executeEmptyR(
                        "write_fst(myFrame, \"" + filePath.replace("\\", "/") + "\")");
            }
        }

        @Test
        void saveFst_throwsWhenFileEmpty() throws Exception {
            Path tempFile = tempDir.resolve("empty.fst");
            Files.writeString(tempFile, "");
            String filePath = tempFile.toAbsolutePath().toString();

            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
                utilMock.when(() -> Utility.normalizePath(anyString())).thenReturn(filePath);

                assertThrows(IllegalArgumentException.class,
                        () -> builder.saveFst(filePath, "myFrame"));
            }
        }
    }

    // ---------------------------------------------------------------
    // getConnection / getPort (remaining branches)
    // ---------------------------------------------------------------
    @Nested
    class GetConnectionBranchTests {

        @Test
        void getConnection_withRJavaRserveTranslator_delegatesToGetConnection() {
            RJavaRserveTranslator rserveTranslator = mock(RJavaRserveTranslator.class);
            RConnection mockConn = mock(RConnection.class);
            when(rserveTranslator.getConnection()).thenReturn(mockConn);

            RFrameBuilder rserveBuilder = new RFrameBuilder(rserveTranslator);
            RConnection result = rserveBuilder.getConnection();

            assertSame(mockConn, result);
            verify(rserveTranslator).getConnection();
        }

        @Test
        void getConnection_withRJavaUserRserveTranslator_delegatesToGetConnection() {
            RJavaUserRserveTranslator userTranslator = mock(RJavaUserRserveTranslator.class);
            RConnection mockConn = mock(RConnection.class);
            when(userTranslator.getConnection()).thenReturn(mockConn);

            RFrameBuilder userBuilder = new RFrameBuilder(userTranslator);
            RConnection result = userBuilder.getConnection();

            assertSame(mockConn, result);
            verify(userTranslator).getConnection();
        }

        @Test
        void getPort_withRJavaRserveTranslator_delegatesToGetPort() {
            RJavaRserveTranslator rserveTranslator = mock(RJavaRserveTranslator.class);
            when(rserveTranslator.getPort()).thenReturn("6311");

            RFrameBuilder rserveBuilder = new RFrameBuilder(rserveTranslator);
            String result = rserveBuilder.getPort();

            assertEquals("6311", result);
            verify(rserveTranslator).getPort();
        }
    }

    // ---------------------------------------------------------------
    // genRowId
    // ---------------------------------------------------------------
    @Nested
    class GenRowIdTests {

        @Test
        void genRowId_generatesRowNamesBindsAndRenames() {
            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class)) {
                utilMock.when(() -> Utility.getRandomString(6))
                        .thenReturn("aaa111")
                        .thenReturn("bbb222");

                builder.genRowId("myTable", "rowId");

                // 1) aaa111<- rownames(myTable);
                verify(mockTranslator).executeEmptyR("aaa111<- rownames(myTable);");
                // 2) bbb222 <- cbind(rowId=aaa111, myTable);
                verify(mockTranslator).executeEmptyR("bbb222 <- cbind(rowId=aaa111, myTable);");
                // 3) myTable <- bbb222;
                verify(mockTranslator).executeEmptyR("myTable <- bbb222;");
            }
        }
    }
}
