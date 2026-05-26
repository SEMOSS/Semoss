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

import java.util.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;

class RSyntaxHelperUnitTests {

    @Nested
    class CreateRColVecArrayTests {
        @Test
        void stringTypes_areQuoted() {
            Object[] row = {"hello", "world"};
            SemossDataType[] types = {SemossDataType.STRING, SemossDataType.STRING};
            assertEquals("c(\"hello\",\"world\")", RSyntaxHelper.createRColVec(row, types));
        }

        @Test
        void factorTypes_areQuoted() {
            Object[] row = {"a", "b"};
            SemossDataType[] types = {SemossDataType.FACTOR, SemossDataType.FACTOR};
            assertEquals("c(\"a\",\"b\")", RSyntaxHelper.createRColVec(row, types));
        }

        @Test
        void numericTypes_notQuoted() {
            Object[] row = {1, 3.14};
            SemossDataType[] types = {SemossDataType.INT, SemossDataType.DOUBLE};
            assertEquals("c(1,3.14)", RSyntaxHelper.createRColVec(row, types));
        }

        @Test
        void mixedTypes() {
            Object[] row = {"name", 42};
            SemossDataType[] types = {SemossDataType.STRING, SemossDataType.INT};
            assertEquals("c(\"name\",42)", RSyntaxHelper.createRColVec(row, types));
        }

        @Test
        void singleElement() {
            Object[] row = {"only"};
            SemossDataType[] types = {SemossDataType.STRING};
            assertEquals("c(\"only\")", RSyntaxHelper.createRColVec(row, types));
        }

        @Test
        void emptyArray() {
            Object[] row = {};
            SemossDataType[] types = {};
            assertEquals("c()", RSyntaxHelper.createRColVec(row, types));
        }
    }

    @Nested
    class CreateRColVecListTests {
        @Test
        void stringDataType_quotesValues() {
            List<Object> row = List.of("hello", "world");
            String result = RSyntaxHelper.createRColVec(row, SemossDataType.STRING, null);
            assertEquals("c(\"hello\",\"world\")", result);
        }

        @Test
        void intDataType_usesDecimalFormat() {
            List<Object> row = List.of(42);
            String result = RSyntaxHelper.createRColVec(row, SemossDataType.INT, null);
            assertTrue(result.startsWith("c("));
            assertTrue(result.contains("42"));
        }

        @Test
        void doubleDataType_usesDecimalFormat() {
            List<Object> row = List.of(3.14);
            String result = RSyntaxHelper.createRColVec(row, SemossDataType.DOUBLE, null);
            assertTrue(result.startsWith("c("));
            assertTrue(result.contains("3.14"));
        }

        @Test
        void dateDataType_usesAsDate() {
            List<Object> row = List.of("2024-01-15");
            String result = RSyntaxHelper.createRColVec(row, SemossDataType.DATE, null);
            assertTrue(result.contains("as.Date("));
            assertTrue(result.contains("2024-01-15"));
        }

        @Test
        void timestampDataType_usesAsPOSIXct() {
            List<Object> row = List.of("2024-01-15 10:30:00");
            String result = RSyntaxHelper.createRColVec(row, SemossDataType.TIMESTAMP, null);
            assertTrue(result.contains("as.POSIXct("));
        }

        @Test
        void timestampDataType_withAdditionalParam() {
            List<Object> row = List.of("2024-01-15 10:30:00");
            String result = RSyntaxHelper.createRColVec(row, SemossDataType.TIMESTAMP, "tz='UTC'");
            assertTrue(result.contains("tz='UTC'"));
        }

        @Test
        void nullDataType_stringInstance_quoted() {
            List<Object> row = new ArrayList<>();
            row.add("text");
            String result = RSyntaxHelper.createRColVec(row, null, null);
            assertEquals("c(\"text\")", result);
        }

        @Test
        void nullDataType_numericInstance_notQuoted() {
            List<Object> row = new ArrayList<>();
            row.add(42);
            String result = RSyntaxHelper.createRColVec(row, null, null);
            assertEquals("c(42)", result);
        }

        @Test
        void factorDataType_quotesValues() {
            List<Object> row = List.of("level1");
            String result = RSyntaxHelper.createRColVec(row, SemossDataType.FACTOR, null);
            assertEquals("c(\"level1\")", result);
        }

        @Test
        void booleanDataType_fallsToElse() {
            List<Object> row = List.of(true);
            String result = RSyntaxHelper.createRColVec(row, SemossDataType.BOOLEAN, null);
            assertEquals("c(true)", result);
        }
    }

    @Nested
    class CreateStringRColVecGenRowStructTests {
        @Test
        void genRowStruct_quotesAllValues() {
            GenRowStruct grs = mock(GenRowStruct.class);
            when(grs.size()).thenReturn(2);
            when(grs.get(0)).thenReturn("col1");
            when(grs.get(1)).thenReturn("col2");
            assertEquals("c(\"col1\",\"col2\")", RSyntaxHelper.createStringRColVec(grs));
        }

        @Test
        void genRowStruct_singleElement() {
            GenRowStruct grs = mock(GenRowStruct.class);
            when(grs.size()).thenReturn(1);
            when(grs.get(0)).thenReturn("only");
            assertEquals("c(\"only\")", RSyntaxHelper.createStringRColVec(grs));
        }
    }

    @Nested
    class CreateStringRColVecListTests {
        @Test
        void listOfStrings_quotesAll() {
            List<String> values = List.of("a", "b", "c");
            assertEquals("c(\"a\",\"b\",\"c\")", RSyntaxHelper.createStringRColVec(values));
        }

        @Test
        void singleString() {
            List<String> values = List.of("only");
            assertEquals("c(\"only\")", RSyntaxHelper.createStringRColVec(values));
        }

        @Test
        void emptyList() {
            List<String> values = List.of();
            assertEquals("c()", RSyntaxHelper.createStringRColVec(values));
        }
    }

    @Nested
    class CreateStringRColVecCollectionTests {
        @Test
        void collection_quotesAll() {
            Collection<String> values = new LinkedHashSet<>(List.of("x", "y"));
            String result = RSyntaxHelper.createStringRColVec(values);
            assertEquals("c(\"x\", \"y\")", result);
        }

        @Test
        void emptyCollection() {
            Collection<String> values = new LinkedHashSet<>();
            assertEquals("c()", RSyntaxHelper.createStringRColVec(values));
        }

        @Test
        void singleElementCollection() {
            Collection<String> values = new LinkedHashSet<>(List.of("solo"));
            assertEquals("c(\"solo\")", RSyntaxHelper.createStringRColVec(values));
        }
    }

    @Nested
    class CreateStringRColVecObjectArrayTests {
        @Test
        void objectArray_quotesAll() {
            Object[] row = {"a", "b", "c"};
            assertEquals("c(\"a\",\"b\",\"c\")", RSyntaxHelper.createStringRColVec(row));
        }

        @Test
        void singleElement() {
            Object[] row = {"only"};
            assertEquals("c(\"only\")", RSyntaxHelper.createStringRColVec(row));
        }
    }

    @Nested
    class CreateStringRColVecIntegerArrayTests {
        @Test
        void integerArray_noQuotes() {
            Integer[] row = {1, 2, 3};
            assertEquals("c(1,2,3)", RSyntaxHelper.createStringRColVec(row));
        }

        @Test
        void singleInteger() {
            Integer[] row = {42};
            assertEquals("c(42)", RSyntaxHelper.createStringRColVec(row));
        }
    }

    @Nested
    class CreateStringRColVecDoubleArrayTests {
        @Test
        void doubleArray_noQuotes() {
            Double[] row = {1.1, 2.2};
            assertEquals("c(1.1,2.2)", RSyntaxHelper.createStringRColVec(row));
        }

        @Test
        void singleDouble() {
            Double[] row = {3.14};
            assertEquals("c(3.14)", RSyntaxHelper.createStringRColVec(row));
        }
    }

    @Nested
    class GetOrderedLevelsTests {
        @Test
        void getOrderedLevelsFromRFactorCol_returnsCorrectSyntax() {
            String result = RSyntaxHelper.getOrderedLevelsFromRFactorCol("myTable", "grade");
            assertEquals("paste(levels(myTable$grade), collapse = '+++');", result);
        }
    }

    @Nested
    class AlterColumnNameTests {
        @Test
        void alterColumnName_generatesCorrectSyntax() {
            String result = RSyntaxHelper.alterColumnName("dt", "oldCol", "newCol");
            assertEquals("colnames(dt)[which(names(dt) == \"oldCol\")] <- \"newCol\";", result);
        }
    }

    @Nested
    class AlterColumnNamesTests {
        @Test
        void alterColumnNames_generatesSetnames() {
            String result = RSyntaxHelper.alterColumnNames("dt",
                    new String[]{"old1", "old2"}, new String[]{"new1", "new2"});
            assertEquals("setnames(dt,old=c(\"old1\",\"old2\"),new=c(\"new1\",\"new2\"));", result);
        }
    }

    @Nested
    class AlterColumnTypeTests {
        @Test
        void int_dispatchesToInteger() {
            String result = RSyntaxHelper.alterColumnType("dt", "col", SemossDataType.INT);
            assertTrue(result.contains("as.integer"));
        }

        @Test
        void double_dispatchesToNumeric() {
            String result = RSyntaxHelper.alterColumnType("dt", "col", SemossDataType.DOUBLE);
            assertTrue(result.contains("as.numeric"));
        }

        @Test
        void string_dispatchesToCharacter() {
            String result = RSyntaxHelper.alterColumnType("dt", "col", SemossDataType.STRING);
            assertTrue(result.contains("as.character"));
        }

        @Test
        void factor_dispatchesToFactor() {
            String result = RSyntaxHelper.alterColumnType("dt", "col", SemossDataType.FACTOR);
            assertTrue(result.contains("as.factor"));
        }

        @Test
        void date_dispatchesToDate() {
            String result = RSyntaxHelper.alterColumnType("dt", "col", SemossDataType.DATE);
            assertTrue(result.contains("as.Date"));
        }

        @Test
        void timestamp_dispatchesToDateTime() {
            String result = RSyntaxHelper.alterColumnType("dt", "col", SemossDataType.TIMESTAMP);
            assertTrue(result.contains("as.POSIXct"));
        }

        @Test
        void boolean_throwsIllegalArgument() {
            assertThrows(IllegalArgumentException.class,
                    () -> RSyntaxHelper.alterColumnType("dt", "col", SemossDataType.BOOLEAN));
        }
    }

    @Nested
    class AlterColumnTypeToCharacterTests {
        @Test
        void singleColumn_generatesCorrectSyntax() {
            String result = RSyntaxHelper.alterColumnTypeToCharacter("dt", "col");
            assertEquals("dt$col <- as.character(dt$col)", result);
        }

        @Test
        void listOfColumns_generatesLapply() {
            String result = RSyntaxHelper.alterColumnTypeToCharacter("dt", List.of("a", "b"));
            assertTrue(result.contains("lapply(.SD, as.character)"));
            assertTrue(result.contains("'a','b'"));
        }
    }

    @Nested
    class AlterColumnTypeToFactorTests {
        @Test
        void singleColumn_generatesCorrectSyntax() {
            String result = RSyntaxHelper.alterColumnTypeToFactor("dt", "col");
            assertEquals("dt$col <- as.factor(dt$col)", result);
        }

        @Test
        void listOfColumns_generatesLapply() {
            String result = RSyntaxHelper.alterColumnTypeToFactor("dt", List.of("x"));
            assertTrue(result.contains("lapply(.SD, as.factor)"));
        }
    }

    @Nested
    class AlterColumnTypeToNumericTests {
        @Test
        void singleColumn_generatesCorrectSyntax() {
            String result = RSyntaxHelper.alterColumnTypeToNumeric("dt", "col");
            assertEquals("dt$col <- as.numeric(as.character(dt$col))", result);
        }

        @Test
        void listOfColumns_generatesLapply() {
            String result = RSyntaxHelper.alterColumnTypeToNumeric("dt", List.of("a", "b"));
            assertTrue(result.contains("lapply(.SD, as.numeric)"));
        }
    }

    @Nested
    class AlterColumnTypeToBooleanTests {
        @Test
        void singleColumn_generatesCorrectSyntax() {
            String result = RSyntaxHelper.alterColumnTypeToBoolean("dt", "col");
            assertEquals("dt$col <- as.logical(dt$col)", result);
        }

        @Test
        void listOfColumns_generatesLapply() {
            String result = RSyntaxHelper.alterColumnTypeToBoolean("dt", List.of("flag"));
            assertTrue(result.contains("lapply(.SD, as.logical)"));
        }
    }

    @Nested
    class AlterColumnTypeToIntegerTests {
        @Test
        void singleColumn_generatesCorrectSyntax() {
            String result = RSyntaxHelper.alterColumnTypeToInteger("dt", "col");
            assertEquals("dt$col <- as.integer(as.character(dt$col))", result);
        }

        @Test
        void listOfColumns_generatesLapply() {
            String result = RSyntaxHelper.alterColumnTypeToInteger("dt", List.of("a"));
            assertTrue(result.contains("lapply(.SD, as.integer)"));
        }
    }

    @Nested
    class AlterColumnTypeToDateTests {
        @Test
        void singleColumn_nullFormat_usesDefault() {
            String result = RSyntaxHelper.alterColumnTypeToDate("dt", null, "col");
            assertTrue(result.contains("as.Date("));
            assertTrue(result.contains("%Y-%m-%d"));
        }

        @Test
        void singleColumn_customFormat() {
            String result = RSyntaxHelper.alterColumnTypeToDate("dt", "%m/%d/%Y", "col");
            assertTrue(result.contains("%m/%d/%Y"));
        }

        @Test
        void singleColumn_formatWithPipe() {
            String result = RSyntaxHelper.alterColumnTypeToDate("dt", "%Y-%m-%d|extra", "col");
            assertTrue(result.contains("%Y-%m-%d"));
            assertFalse(result.contains("extra"));
        }

        @Test
        void listOfColumns_nullFormat_usesDefault() {
            String result = RSyntaxHelper.alterColumnTypeToDate("dt", null, List.of("d1", "d2"));
            assertTrue(result.contains("lapply(.SD"));
            assertTrue(result.contains("%Y-%m-%d"));
        }

        @Test
        void listOfColumns_customFormat() {
            String result = RSyntaxHelper.alterColumnTypeToDate("dt", "%d-%b-%Y", List.of("d1"));
            assertTrue(result.contains("%d-%b-%Y"));
        }
    }

    @Nested
    class AlterColumnTypeToDateTimeTests {
        @Test
        void singleColumn_nullFormat_usesDefault() {
            String result = RSyntaxHelper.alterColumnTypeToDateTime("dt", null, "col");
            assertTrue(result.contains("as.POSIXct"));
            assertTrue(result.contains("fast_strptime"));
            assertTrue(result.contains("digits.secs=NULL"));
        }

        @Test
        void singleColumn_customFormat() {
            String result = RSyntaxHelper.alterColumnTypeToDateTime("dt", "%Y-%m-%d %H:%M:%OS|3", "col");
            assertTrue(result.contains("digits.secs=3"));
            assertTrue(result.contains("%Y-%m-%d %H:%M:%OS"));
        }

        @Test
        void listOfColumns_nullFormat() {
            String result = RSyntaxHelper.alterColumnTypeToDateTime("dt", null, List.of("ts1"));
            assertTrue(result.contains("lapply(.SD"));
            assertTrue(result.contains("fast_strptime"));
        }

        @Test
        void listOfColumns_customFormat() {
            String result = RSyntaxHelper.alterColumnTypeToDateTime("dt", "%Y-%m-%d %H:%M:%S|0", List.of("ts1", "ts2"));
            assertTrue(result.contains("digits.secs=0"));
        }
    }

    @Nested
    class AlterEmptyTableColumnTypeToDateTimeTests {
        @Test
        void generatesLapplyAsPOSIXct() {
            String result = RSyntaxHelper.alterEmptyTableColumnTypeToDateTime("dt", List.of("ts1", "ts2"));
            assertTrue(result.contains("lapply(.SD, function(x) as.POSIXct(x))"));
            assertTrue(result.contains("'ts1','ts2'"));
        }
    }

    @Nested
    class GetFrameSubsetTests {
        @Test
        void generatesSubsetSyntax() {
            String result = RSyntaxHelper.getFrameSubset("result", "df", new Object[]{"col1", "col2"});
            assertEquals("result<- subset(df, select=c(\"col1\",\"col2\"));", result);
        }
    }

    @Nested
    class AsDataTableTests {
        @Test
        void generatesCorrectSyntax() {
            assertEquals("newDT<- as.data.table(oldDF);", RSyntaxHelper.asDataTable("newDT", "oldDF"));
        }
    }

    @Nested
    class AsDataFrameTests {
        @Test
        void generatesCorrectSyntax() {
            assertEquals("newDF<- as.data.frame(oldDT);", RSyntaxHelper.asDataFrame("newDF", "oldDT"));
        }
    }

    @Nested
    class ExtractNumbersTests {
        @Test
        void generatesGsubSyntax() {
            String result = RSyntaxHelper.extractNumbers("df", "price");
            assertTrue(result.contains("as.numeric(gsub("));
            assertTrue(result.contains("df$price"));
        }
    }

    @Nested
    class GetMergeSyntaxTests {
        @Test
        void innerJoin() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> join = new LinkedHashMap<>();
            join.put("id", "fk_id");
            joinCols.add(join);

            String result = RSyntaxHelper.getMergeSyntax("result", "left", "right", "inner.join", joinCols);
            assertTrue(result.contains("merge(left, right"));
            assertTrue(result.contains("by.x = c(\"id\")"));
            assertTrue(result.contains("by.y = c(\"fk_id\")"));
            assertTrue(result.contains("all = FALSE"));
        }

        @Test
        void leftOuterJoin() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> join = new LinkedHashMap<>();
            join.put("id", "fk");
            joinCols.add(join);

            String result = RSyntaxHelper.getMergeSyntax("r", "l", "r2", "left.outer.join", joinCols);
            assertTrue(result.contains("all.x = TRUE"));
            assertTrue(result.contains("all.y = FALSE"));
        }

        @Test
        void rightOuterJoin() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> join = new LinkedHashMap<>();
            join.put("id", "fk");
            joinCols.add(join);

            String result = RSyntaxHelper.getMergeSyntax("r", "l", "r2", "right.outer.join", joinCols);
            assertTrue(result.contains("all.x = FALSE"));
            assertTrue(result.contains("all.y = TRUE"));
        }

        @Test
        void outerJoin() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> join = new LinkedHashMap<>();
            join.put("id", "fk");
            joinCols.add(join);

            String result = RSyntaxHelper.getMergeSyntax("r", "l", "r2", "outer.join", joinCols);
            assertTrue(result.contains("all = TRUE"));
        }

        @Test
        void multipleJoinColumns() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> join1 = new LinkedHashMap<>();
            join1.put("id1", "fk1");
            joinCols.add(join1);
            Map<String, String> join2 = new LinkedHashMap<>();
            join2.put("id2", "fk2");
            joinCols.add(join2);

            String result = RSyntaxHelper.getMergeSyntax("r", "l", "r2", "inner.join", joinCols);
            assertTrue(result.contains("\"id1\""));
            assertTrue(result.contains("\"id2\""));
        }

        @Test
        void unknownJoinType_noAllClause() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> join = new LinkedHashMap<>();
            join.put("id", "fk");
            joinCols.add(join);

            String result = RSyntaxHelper.getMergeSyntax("r", "l", "r2", "cross.join", joinCols);
            // Unknown join type - none of the if-else branches append a closing clause
            assertFalse(result.contains("all"));
        }
    }

    @Nested
    class CreateOrderedRFactorTests {
        @Test
        void createOrderedRFactor_invalidRegexInSplit_throwsPatternSyntaxException() {
            // The method uses split("+++") which is an invalid regex pattern
            assertThrows(java.util.regex.PatternSyntaxException.class,
                    () -> RSyntaxHelper.createOrderedRFactor(
                            new Object[]{"a", "b"}, "low+++high"));
        }

        @Test
        void createOrderedRFactor_withLabels_invalidRegexInSplit_throwsPatternSyntaxException() {
            assertThrows(java.util.regex.PatternSyntaxException.class,
                    () -> RSyntaxHelper.createOrderedRFactor(
                            new Object[]{"a", "b"}, "low+++high", "L+++H"));
        }
    }

    @Nested
    class GetMergeColsSyntaxTests {
        @Test
        void grabKeys_true() {
            StringBuilder sb = new StringBuilder();
            List<Map<String, String>> cols = new ArrayList<>();
            Map<String, String> m = new LinkedHashMap<>();
            m.put("leftCol", "rightCol");
            cols.add(m);
            RSyntaxHelper.getMergeColsSyntax(sb, cols, true);
            assertEquals("\"leftCol\"", sb.toString());
        }

        @Test
        void grabKeys_false() {
            StringBuilder sb = new StringBuilder();
            List<Map<String, String>> cols = new ArrayList<>();
            Map<String, String> m = new LinkedHashMap<>();
            m.put("leftCol", "rightCol");
            cols.add(m);
            RSyntaxHelper.getMergeColsSyntax(sb, cols, false);
            assertEquals("\"rightCol\"", sb.toString());
        }

        @Test
        void emptyMap_skipped() {
            StringBuilder sb = new StringBuilder();
            List<Map<String, String>> cols = new ArrayList<>();
            cols.add(new LinkedHashMap<>());
            Map<String, String> m = new LinkedHashMap<>();
            m.put("col", "fk");
            cols.add(m);
            RSyntaxHelper.getMergeColsSyntax(sb, cols, true);
            assertEquals("\"col\"", sb.toString());
        }
    }

    @Nested
    class AlterMissingColumnsTests {
        @Test
        void skipsJoinColumns() {
            Map<String, SemossDataType> newCols = new LinkedHashMap<>();
            newCols.put("joinCol", SemossDataType.STRING);
            newCols.put("newCol", SemossDataType.INT);

            Join j = mock(Join.class);
            when(j.getRColumn()).thenReturn("joinCol");

            String result = RSyntaxHelper.alterMissingColumns("dt", newCols, List.of(j), new HashMap<>());
            assertFalse(result.contains("joinCol"));
            assertTrue(result.contains("as.integer(dt$newCol)"));
        }

        @Test
        void appliesAlias() {
            Map<String, SemossDataType> newCols = new LinkedHashMap<>();
            newCols.put("origCol", SemossDataType.STRING);

            Map<String, String> aliases = new HashMap<>();
            aliases.put("origCol", "aliasCol");

            String result = RSyntaxHelper.alterMissingColumns("dt", newCols, List.of(), aliases);
            assertTrue(result.contains("dt$aliasCol"));
        }

        @Test
        void doubleType_usesAsNumeric() {
            Map<String, SemossDataType> newCols = new LinkedHashMap<>();
            newCols.put("val", SemossDataType.DOUBLE);

            String result = RSyntaxHelper.alterMissingColumns("dt", newCols, List.of(), new HashMap<>());
            assertTrue(result.contains("as.numeric(dt$val)"));
        }

        @Test
        void intType_usesAsInteger() {
            Map<String, SemossDataType> newCols = new LinkedHashMap<>();
            newCols.put("count", SemossDataType.INT);

            String result = RSyntaxHelper.alterMissingColumns("dt", newCols, List.of(), new HashMap<>());
            assertTrue(result.contains("as.integer(dt$count)"));
        }

        @Test
        void otherType_usesAsCharacter() {
            Map<String, SemossDataType> newCols = new LinkedHashMap<>();
            newCols.put("flag", SemossDataType.BOOLEAN);

            String result = RSyntaxHelper.alterMissingColumns("dt", newCols, List.of(), new HashMap<>());
            assertTrue(result.contains("as.character(dt$flag)"));
        }

        @Test
        void headerWithDoubleUnderscore_stripped() {
            Map<String, SemossDataType> newCols = new LinkedHashMap<>();
            newCols.put("table__col", SemossDataType.STRING);

            String result = RSyntaxHelper.alterMissingColumns("dt", newCols, List.of(), new HashMap<>());
            assertTrue(result.contains("dt$col"));
        }

        @Test
        void joinColumnWithDoubleUnderscore_stripped() {
            Map<String, SemossDataType> newCols = new LinkedHashMap<>();
            newCols.put("table__joinCol", SemossDataType.STRING);

            Join j = mock(Join.class);
            when(j.getRColumn()).thenReturn("other__joinCol");

            String result = RSyntaxHelper.alterMissingColumns("dt", newCols, List.of(j), new HashMap<>());
            assertFalse(result.contains("joinCol"));
        }

        @Test
        void noNewColumns_returnsEmpty() {
            String result = RSyntaxHelper.alterMissingColumns("dt", new LinkedHashMap<>(), List.of(), new HashMap<>());
            assertEquals("", result);
        }
    }

    @Nested
    class GetFReadSyntaxTests {
        @Test
        void defaultDelimiter() {
            String result = RSyntaxHelper.getFReadSyntax("dt", "/path/to/file.csv");
            assertTrue(result.contains("fread("));
            assertTrue(result.contains("sep=\",\""));
            assertTrue(result.contains("/path/to/file.csv"));
        }

        @Test
        void customDelimiter() {
            String result = RSyntaxHelper.getFReadSyntax("dt", "/path/file.tsv", "\\t");
            assertTrue(result.contains("sep=\"\\t\""));
        }

        @Test
        void backslashesReplaced() {
            String result = RSyntaxHelper.getFReadSyntax("dt", "C:\\Users\\data\\file.csv");
            assertTrue(result.contains("C:/Users/data/file.csv"));
            assertFalse(result.contains("\\"));
        }

        @Test
        void containsExpectedOptions() {
            String result = RSyntaxHelper.getFReadSyntax("dt", "file.csv");
            assertTrue(result.contains("encoding=\"UTF-8\""));
            assertTrue(result.contains("blank.lines.skip=TRUE"));
            assertTrue(result.contains("fill=TRUE"));
            assertTrue(result.contains("keepLeadingZeros=TRUE"));
        }
    }

    @Nested
    class GetFWriteSyntaxTests {
        @Test
        void generatesCorrectSyntax() {
            String result = RSyntaxHelper.getFWriteSyntax("df", "/out/data.csv");
            assertEquals("fwrite(df,file=\"/out/data.csv\");", result);
        }

        @Test
        void replacesBackslashes() {
            String result = RSyntaxHelper.getFWriteSyntax("df", "C:\\out\\data.csv");
            assertTrue(result.contains("C:/out/data.csv"));
        }
    }

    @Nested
    class GetExcelReadSheetSyntaxTests {
        @Test
        void withSubset() {
            String result = RSyntaxHelper.getExcelReadSheetSyntax("dt", "/file.xlsx", 1,
                    List.of(1, 2, 3), true);
            assertTrue(result.contains("read.xlsx2("));
            assertTrue(result.contains("sheetIndex=1"));
            assertTrue(result.contains("colIndex = c(1,2,3)"));
        }

        @Test
        void withoutSubset() {
            String result = RSyntaxHelper.getExcelReadSheetSyntax("dt", "/file.xlsx", 2,
                    List.of(1, 2), false);
            assertTrue(result.contains("sheetIndex=2"));
            assertFalse(result.contains("colIndex"));
        }

        @Test
        void backslashesReplaced() {
            String result = RSyntaxHelper.getExcelReadSheetSyntax("dt", "C:\\file.xlsx", 1,
                    List.of(1), true);
            assertTrue(result.contains("C:/file.xlsx"));
        }
    }

    @Nested
    class LoadExcelSheetTests {
        @Test
        void generatesCorrectSyntax() {
            String result = RSyntaxHelper.loadExcelSheet("/data/file.xlsx", "myFrame", "Sheet1", "A1:Z100");
            assertTrue(result.contains("library(readxl)"));
            assertTrue(result.contains("library(cellranger)"));
            assertTrue(result.contains("read_excel("));
            assertTrue(result.contains("sheet = \"Sheet1\""));
            assertTrue(result.contains("range='A1:Z100'"));
            assertTrue(result.contains("as.data.table(myFrame)"));
        }

        @Test
        void backslashesReplaced() {
            String result = RSyntaxHelper.loadExcelSheet("C:\\data\\file.xlsx", "f", "S1", "A1:B2");
            assertTrue(result.contains("C:/data/file.xlsx"));
        }
    }

    @Nested
    class LoadParquetFileTests {
        @Test
        void generatesCorrectSyntax() {
            String result = RSyntaxHelper.loadParquetFile("/data/file.parquet", "myFrame");
            assertTrue(result.contains("library(arrow)"));
            assertTrue(result.contains("read_parquet("));
            assertTrue(result.contains("as.data.table(myFrame)"));
        }

        @Test
        void backslashesReplaced() {
            String result = RSyntaxHelper.loadParquetFile("C:\\data\\file.parquet", "f");
            assertTrue(result.contains("C:/data/file.parquet"));
        }
    }

    @Nested
    class FormatFilterValueTests {
        @Test
        void stringType_quoted() {
            assertEquals("\"hello\"", RSyntaxHelper.formatFilterValue("hello", SemossDataType.STRING, null));
        }

        @Test
        void factorType_quoted() {
            assertEquals("\"level1\"", RSyntaxHelper.formatFilterValue("level1", SemossDataType.FACTOR, null));
        }

        @Test
        void intType_formatsDecimal() {
            String result = RSyntaxHelper.formatFilterValue(42, SemossDataType.INT, null);
            assertTrue(result.contains("42"));
        }

        @Test
        void doubleType_formatsDecimal() {
            String result = RSyntaxHelper.formatFilterValue(3.14, SemossDataType.DOUBLE, null);
            assertTrue(result.contains("3.14"));
        }

        @Test
        void dateType_usesAsDate() {
            String result = RSyntaxHelper.formatFilterValue("2024-01-15", SemossDataType.DATE, null);
            assertTrue(result.contains("as.Date("));
        }

        @Test
        void timestampType_usesAsPOSIXct() {
            String result = RSyntaxHelper.formatFilterValue("2024-01-15 10:30:00", SemossDataType.TIMESTAMP, null);
            assertTrue(result.contains("as.POSIXct("));
        }

        @Test
        void timestampType_withAdditionalParam() {
            String result = RSyntaxHelper.formatFilterValue("2024-01-15", SemossDataType.TIMESTAMP, "tz='UTC'");
            assertTrue(result.contains("tz='UTC'"));
        }

        @Test
        void nullType_stringValue_quoted() {
            assertEquals("\"text\"", RSyntaxHelper.formatFilterValue("text", null, null));
        }

        @Test
        void nullType_nonStringValue_toString() {
            assertEquals("42", RSyntaxHelper.formatFilterValue(42, null, null));
        }

        @Test
        void booleanType_fallsToElse() {
            String result = RSyntaxHelper.formatFilterValue(true, SemossDataType.BOOLEAN, null);
            assertEquals("true", result);
        }
    }

    @Nested
    class EscapeRegexRTests {
        @Test
        void nonGrepExpression_unchanged() {
            String result = RSyntaxHelper.escapeRegexR("x <- 1");
            assertEquals("x <- 1", result);
        }

        @Test
        void greplExpression_escapesBackslashes() {
            String expr = "grepl(\\\"test\\\\d\\\", col)";
            String result = RSyntaxHelper.escapeRegexR(expr);
            assertNotNull(result);
        }

        @Test
        void emptyString_unchanged() {
            assertEquals("", RSyntaxHelper.escapeRegexR(""));
        }
    }

    @Nested
    class ReadRDSTests {
        @Test
        void generatesCorrectSyntax() {
            String result = RSyntaxHelper.readRDS("myVar", "/data/file.rds");
            assertEquals("myVar <-readRDS(\"/data/file.rds\");", result);
        }

        @Test
        void replacesBackslashes() {
            String result = RSyntaxHelper.readRDS("v", "C:\\data\\file.rds");
            assertTrue(result.contains("C:/data/file.rds"));
        }
    }

    @Nested
    class QreadTests {
        @Test
        void generatesCorrectSyntax() {
            String result = RSyntaxHelper.qread("myVar", "/data/file.qs");
            assertEquals("myVar <-qread(\"/data/file.qs\");", result);
        }

        @Test
        void replacesBackslashes() {
            String result = RSyntaxHelper.qread("v", "C:\\data\\f.qs");
            assertTrue(result.contains("C:/data/f.qs"));
        }
    }

    @Nested
    class TranslateJavaRDateTimeFormatTests {
        @Test
        void simpleDate_yyyyMMdd() {
            String result = RSyntaxHelper.translateJavaRDateTimeFormat("yyyy-MM-dd");
            assertEquals("%Y-%m-%d|NULL", result);
        }

        @Test
        void dateWithSlashes() {
            String result = RSyntaxHelper.translateJavaRDateTimeFormat("MM/dd/yyyy");
            assertEquals("%m/%d/%Y|NULL", result);
        }

        @Test
        void twoDigitYear() {
            String result = RSyntaxHelper.translateJavaRDateTimeFormat("yy-MM-dd");
            assertEquals("%y-%m-%d|NULL", result);
        }

        @Test
        void dateTime_yyyyMMddHHmmss() {
            String result = RSyntaxHelper.translateJavaRDateTimeFormat("yyyy-MM-dd HH:mm:ss");
            assertEquals("%Y-%m-%d %H:%M:%S|NULL", result);
        }

        @Test
        void abbreviatedMonth() {
            String result = RSyntaxHelper.translateJavaRDateTimeFormat("dd-MMM-yyyy");
            assertEquals("%d-%b-%Y|NULL", result);
        }

        @Test
        void fullMonthName() {
            String result = RSyntaxHelper.translateJavaRDateTimeFormat("dd-MMMM-yyyy");
            assertEquals("%d-%B-%Y|NULL", result);
        }

        @Test
        void abbreviatedDayName() {
            String result = RSyntaxHelper.translateJavaRDateTimeFormat("EEE");
            assertEquals("%a|NULL", result);
        }

        @Test
        void fullDayName() {
            String result = RSyntaxHelper.translateJavaRDateTimeFormat("EEEE");
            assertEquals("%A|NULL", result);
        }

        @Test
        void hourMinute_12hour() {
            String result = RSyntaxHelper.translateJavaRDateTimeFormat("hh:mm a");
            assertEquals("%I:%M %p|NULL", result);
        }

        @Test
        void dayInYear() {
            String result = RSyntaxHelper.translateJavaRDateTimeFormat("D");
            assertEquals("%j|NULL", result);
        }

        @Test
        void dayNumberOfWeek() {
            String result = RSyntaxHelper.translateJavaRDateTimeFormat("u");
            assertEquals("%u|NULL", result);
        }

        @Test
        void timeZone_Z() {
            String result = RSyntaxHelper.translateJavaRDateTimeFormat("yyyy-MM-dd Z");
            assertEquals("%Y-%m-%d %z|NULL", result);
        }

        @Test
        void withMilliseconds() {
            String result = RSyntaxHelper.translateJavaRDateTimeFormat("yyyy-MM-dd HH:mm:ss.SSS");
            assertTrue(result.contains("%OS"));
            assertTrue(result.contains("|3"));
        }
    }

    @Nested
    class GetValueJavaRDatTimeTranslationMapTests {
        @Test
        void knownKey_y2() {
            assertEquals("%y", RSyntaxHelper.getValueJavaRDatTimeTranslationMap("y2"));
        }

        @Test
        void knownKey_Y4() {
            assertEquals("%Y", RSyntaxHelper.getValueJavaRDatTimeTranslationMap("Y4"));
        }

        @Test
        void knownKey_M1() {
            assertEquals("%m", RSyntaxHelper.getValueJavaRDatTimeTranslationMap("M1"));
        }

        @Test
        void knownKey_M3() {
            assertEquals("%b", RSyntaxHelper.getValueJavaRDatTimeTranslationMap("M3"));
        }

        @Test
        void knownKey_d() {
            assertEquals("%d", RSyntaxHelper.getValueJavaRDatTimeTranslationMap("d"));
        }

        @Test
        void knownKey_H() {
            assertEquals("%H", RSyntaxHelper.getValueJavaRDatTimeTranslationMap("H"));
        }

        @Test
        void knownKey_m() {
            assertEquals("%M", RSyntaxHelper.getValueJavaRDatTimeTranslationMap("m"));
        }

        @Test
        void knownKey_s() {
            assertEquals("%S", RSyntaxHelper.getValueJavaRDatTimeTranslationMap("s"));
        }

        @Test
        void knownKey_S() {
            assertEquals("%OS", RSyntaxHelper.getValueJavaRDatTimeTranslationMap("S"));
        }

        @Test
        void unknownKey_returnsNull() {
            assertNull(RSyntaxHelper.getValueJavaRDatTimeTranslationMap("unknown"));
        }
    }

    @Nested
    class DetermineLimitOffsetSyntaxTests {
        @Test
        void limitOnly_withinBounds() {
            String result = RSyntaxHelper.determineLimitOffsetSyntax("dt", 100, 10, 0);
            assertEquals("dt <- dt[1:10]", result);
        }

        @Test
        void limitOnly_exceedsRows() {
            String result = RSyntaxHelper.determineLimitOffsetSyntax("dt", 5, 10, 0);
            assertEquals("dt <- dt[1:5]", result);
        }

        @Test
        void offsetOnly() {
            String result = RSyntaxHelper.determineLimitOffsetSyntax("dt", 100, 0, 5);
            assertEquals("dt <- dt[6:100]", result);
        }

        @Test
        void limitAndOffset_withinBounds() {
            String result = RSyntaxHelper.determineLimitOffsetSyntax("dt", 100, 10, 5);
            assertEquals("dt <- dt[6:15]", result);
        }

        @Test
        void limitAndOffset_exceedsRows() {
            String result = RSyntaxHelper.determineLimitOffsetSyntax("dt", 12, 10, 5);
            assertEquals("dt <- dt[6:12]", result);
        }

        @Test
        void limitAndOffset_noData_throws() {
            assertThrows(IllegalArgumentException.class,
                    () -> RSyntaxHelper.determineLimitOffsetSyntax("dt", 3, 10, 5));
        }

        @Test
        void noLimitNoOffset() {
            String result = RSyntaxHelper.determineLimitOffsetSyntax("dt", 100, 0, 0);
            assertEquals("dt <- dt", result);
        }

        @Test
        void negativeLimitNoOffset() {
            String result = RSyntaxHelper.determineLimitOffsetSyntax("dt", 100, -1, 0);
            assertEquals("dt <- dt", result);
        }
    }

    @Nested
    class SetWorkingDirectoryTests {
        @Test
        void generatesSetwd() {
            String result = RSyntaxHelper.setWorkingDirectory("/home/user/project");
            assertEquals("setwd(\"/home/user/project\");", result);
        }

        @Test
        void replacesBackslashes() {
            String result = RSyntaxHelper.setWorkingDirectory("C:\\Users\\project");
            assertEquals("setwd(\"C:/Users/project\");", result);
        }
    }

    @Nested
    class GetWorkingDirectoryTests {
        @Test
        void generatesGetwd() {
            assertEquals("getwd();", RSyntaxHelper.getWorkingDirectory());
        }
    }

    @Nested
    class LoadPackagesTests {
        @Test
        void multiplePackages() {
            String result = RSyntaxHelper.loadPackages(new String[]{"dplyr", "ggplot2"});
            assertEquals("library(dplyr);library(ggplot2);", result);
        }

        @Test
        void singlePackage() {
            String result = RSyntaxHelper.loadPackages(new String[]{"data.table"});
            assertEquals("library(data.table);", result);
        }

        @Test
        void emptyArray() {
            String result = RSyntaxHelper.loadPackages(new String[]{});
            assertEquals("", result);
        }
    }

    @Nested
    class LoadLibraryTests {
        @Test
        void generatesCorrectSyntax() {
            assertEquals("library(tidyverse);", RSyntaxHelper.loadLibrary("tidyverse"));
        }
    }

    @Nested
    class ReplaceNAStringTests {
        @Test
        void generatesReplaceSyntax() {
            String result = RSyntaxHelper.replaceNAString("dt", List.of("name", "age"));
            assertTrue(result.contains("replace("));
            assertTrue(result.contains("is.na("));
            assertTrue(result.contains("\"NA\""));
        }

        @Test
        void usesStringRColVec() {
            String result = RSyntaxHelper.replaceNAString("dt", List.of("col1"));
            assertTrue(result.contains("c(\"col1\")"));
        }
    }
}
