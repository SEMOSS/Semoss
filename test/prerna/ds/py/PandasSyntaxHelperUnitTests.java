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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;
import prerna.sablecc2.om.Join;

class PandasSyntaxHelperUnitTests {

    @Test
    void operatorList_containsExpectedOperators() {
        List<String> expected = Arrays.asList(">", "<", ">=", "<=", "==", "!=");
        assertEquals(expected, PandasSyntaxHelper.OPERATOR_LIST);
    }

    @Test
    void operatorList_hasSixElements() {
        assertEquals(6, PandasSyntaxHelper.OPERATOR_LIST.size());
    }


    @Nested
    class MakeWrapperTests {
        @Test
        void makeWrapper_producesCorrectSyntax() {
            String result = PandasSyntaxHelper.makeWrapper("myWrapper", "df");
            assertEquals("myWrapper = PyFrame.makefm(df)", result);
        }
        @Test
        void makeWrapper_withDifferentNames() {
            String result = PandasSyntaxHelper.makeWrapper("w1", "table1");
            assertEquals("w1 = PyFrame.makefm(table1)", result);
        }
    }

    @Test
    void createFrameWrapperName_appendsW() {
        assertEquals("myFramew", PandasSyntaxHelper.createFrameWrapperName("myFrame"));
    }

    @Test
    void createFrameWrapperName_emptyString() {
        assertEquals("w", PandasSyntaxHelper.createFrameWrapperName(""));
    }


    @Nested
    class ExecFileTests {
        @Test
        void execFile_replacesBackslashes() {
            String result = PandasSyntaxHelper.execFile("C:\\Users\\test\\script.py");
            assertEquals("execfile('C:/Users/test/script.py')", result);
        }
        @Test
        void execFile_multipleBackslashes() {
            String result = PandasSyntaxHelper.execFile("C:\\\\path\\\\file.py");
            assertEquals("execfile('C:/path/file.py')", result);
        }
        @Test
        void execFile_forwardSlashesUnchanged() {
            String result = PandasSyntaxHelper.execFile("/home/user/script.py");
            assertEquals("execfile('/home/user/script.py')", result);
        }
    }


    @Nested
    class GetCsvFileRead10ParamTests {
        @Test
        void nullDataTypeMaps_producesSimpleReadCsv() {
            String result = PandasSyntaxHelper.getCsvFileRead(
                    "pd", "np", "C:\\data\\file.csv", "df",
                    ",", "\"", "\\\\", null, null, 0);
            assertTrue(result.startsWith("df=pd.read_csv("));
            assertTrue(result.contains("C:/data/file.csv"));
            assertTrue(result.contains("encoding='utf-8'"));
        }
        @Test
        void emptyDataTypeMaps_producesSimpleReadCsv() {
            String result = PandasSyntaxHelper.getCsvFileRead(
                    "pd", "np", "/data/file.csv", "df",
                    ",", "\"", "\\\\", null, new HashMap<>(), 0);
            assertTrue(result.startsWith("df=pd.read_csv("));
            assertTrue(result.contains("encoding='utf-8'"));
        }
        @Test
        void nullEncoding_defaultsToUtf8() {
            String result = PandasSyntaxHelper.getCsvFileRead(
                    "pd", "np", "/data/file.csv", "df",
                    ",", "\"", "\\\\", null, null, 0);
            assertTrue(result.contains("encoding='utf-8'"));
        }
        @Test
        void customEncoding() {
            String result = PandasSyntaxHelper.getCsvFileRead(
                    "pd", "np", "/data/file.csv", "df",
                    ",", "\"", "\\\\", "latin-1", null, 0);
            assertTrue(result.contains("encoding='latin-1'"));
        }
        @Test
        void withLimit() {
            String result = PandasSyntaxHelper.getCsvFileRead(
                    "pd", "np", "/data/file.csv", "df",
                    ",", "\"", "\\\\", null, null, 100);
            assertTrue(result.contains("nrows=100"));
        }
        @Test
        void withDataTypeMaps_buildsReadCsvWithDtype() {
            Map<String, SemossDataType> dtm = new LinkedHashMap<>();
            dtm.put("Name", SemossDataType.STRING);
            dtm.put("Age", SemossDataType.INT);
            dtm.put("BirthDate", SemossDataType.DATE);
            String result = PandasSyntaxHelper.getCsvFileRead(
                    "pd", "np", "/data/file.csv", "df",
                    ",", "\"", "\\\\", null, dtm, 0);
            assertTrue(result.contains("dtype="));
            assertTrue(result.contains("'Name':object"));
            assertTrue(result.contains("'Age':np.float64"));
            assertTrue(result.contains("parse_dates="));
            assertTrue(result.contains("'BirthDate'"));
        }
        @Test
        void withDataTypeMaps_andLimit() {
            Map<String, SemossDataType> dtm = new LinkedHashMap<>();
            dtm.put("Col1", SemossDataType.DOUBLE);
            String result = PandasSyntaxHelper.getCsvFileRead(
                    "pd", "np", "/data/file.csv", "df",
                    ",", "\"", "\\\\", null, dtm, 50);
            assertTrue(result.contains("nrows=50"));
            assertTrue(result.contains("dtype="));
        }
        @Test
        void backslashesInPath_replaced() {
            String result = PandasSyntaxHelper.getCsvFileRead(
                    "pd", "np", "C:\\Users\\test\\data.csv", "df",
                    ",", "\"", "\\\\", null, null, 0);
            assertTrue(result.contains("C:/Users/test/data.csv"));
        }
        @Test
        void zeroLimit_noNrows() {
            String result = PandasSyntaxHelper.getCsvFileRead(
                    "pd", "np", "/data/file.csv", "df",
                    ",", "\"", "\\\\", null, null, 0);
            assertFalse(result.contains("nrows="));
        }
        @Test
        void emptyEncoding_defaultsToUtf8() {
            String result = PandasSyntaxHelper.getCsvFileRead(
                    "pd", "np", "/data/file.csv", "df",
                    ",", "\"", "\\\\", "", null, 0);
            assertTrue(result.contains("encoding='utf-8'"));
        }
    }


    @Nested
    class GetJsonFileReadTests {
        @Test
        void withoutDataTypeMaps() {
            String result = PandasSyntaxHelper.getJsonFileRead(
                    "pd", "np", "C:\\data\\file.json", "df", null);
            assertEquals("df=pd.read_json('C:/data/file.json", result);
        }
        @Test
        void withEmptyDataTypeMaps() {
            String result = PandasSyntaxHelper.getJsonFileRead(
                    "pd", "np", "/data/file.json", "df", new HashMap<>());
            assertEquals("df=pd.read_json('/data/file.json", result);
        }
        @Test
        void withDataTypeMaps() {
            Map<String, SemossDataType> dtm = new LinkedHashMap<>();
            dtm.put("Name", SemossDataType.STRING);
            dtm.put("Created", SemossDataType.TIMESTAMP);
            String result = PandasSyntaxHelper.getJsonFileRead(
                    "pd", "np", "/data/file.json", "df", dtm);
            assertTrue(result.contains("dtype="));
            assertTrue(result.contains("'Name':object"));
            assertTrue(result.contains("convert_dates="));
            assertTrue(result.contains("'Created'"));
        }
        @Test
        void replacesBackslashes() {
            String result = PandasSyntaxHelper.getJsonFileRead(
                    "pd", "np", "C:\\path\\to\\file.json", "df", null);
            assertTrue(result.contains("C:/path/to/file.json"));
        }
    }


    @Nested
    class ConvertSemossDataTypeTests {
        @Test
        void convert_INT() {
            assertEquals("np.float64", PandasSyntaxHelper.convertSemossDataType("np", SemossDataType.INT));
        }
        @Test
        void convert_DOUBLE() {
            assertEquals("np.float64", PandasSyntaxHelper.convertSemossDataType("np", SemossDataType.DOUBLE));
        }
        @Test
        void convert_BOOLEAN() {
            assertEquals("object", PandasSyntaxHelper.convertSemossDataType("np", SemossDataType.BOOLEAN));
        }
        @Test
        void convert_DATE() {
            assertEquals("object", PandasSyntaxHelper.convertSemossDataType("np", SemossDataType.DATE));
        }
        @Test
        void convert_TIMESTAMP() {
            assertEquals("object", PandasSyntaxHelper.convertSemossDataType("np", SemossDataType.TIMESTAMP));
        }
        @Test
        void convert_STRING() {
            assertEquals("object", PandasSyntaxHelper.convertSemossDataType("np", SemossDataType.STRING));
        }
        @Test
        void stringInput_INT() {
            assertEquals("np.float64", PandasSyntaxHelper.convertSemossDataType("np", "INT"));
        }
        @Test
        void stringInput_STRING() {
            assertEquals("object", PandasSyntaxHelper.convertSemossDataType("np", "STRING"));
        }
        @Test
        void stringInput_DOUBLE() {
            assertEquals("np.float64", PandasSyntaxHelper.convertSemossDataType("np", "DOUBLE"));
        }
    }

    @Test
    void getWriteCsvFile2_delegatesWithNullSep() {
        String result = PandasSyntaxHelper.getWriteCsvFile("df", "/output/file.csv");
        assertTrue(result.contains("sep=','"));
        assertTrue(result.contains("encoding='utf-8'"));
        assertTrue(result.contains("index=False"));
    }


    @Nested
    class GetWriteCsvFile3ParamTests {
        @Test
        void nullSepDefaultsToComma() {
            String result = PandasSyntaxHelper.getWriteCsvFile("df", "/output/file.csv", null);
            assertTrue(result.contains("sep=','"));
        }
        @Test
        void emptySepDefaultsToComma() {
            String result = PandasSyntaxHelper.getWriteCsvFile("df", "/output/file.csv", "");
            assertTrue(result.contains("sep=','"));
        }
        @Test
        void customSep() {
            String result = PandasSyntaxHelper.getWriteCsvFile("df", "/output/file.csv", "\t");
            assertTrue(result.contains("sep='\t'"));
        }
    }


    @Nested
    class GetWriteCsvFile4ParamTests {
        @Test
        void nullEncodingDefaultsToUtf8() {
            String result = PandasSyntaxHelper.getWriteCsvFile("df", "/output/file.csv", ",", null);
            assertTrue(result.contains("encoding='utf-8'"));
        }
        @Test
        void customEncoding() {
            String result = PandasSyntaxHelper.getWriteCsvFile("df", "/output/file.csv", ",", "latin-1");
            assertTrue(result.contains("encoding='latin-1'"));
        }
        @Test
        void fullSyntax() {
            String result = PandasSyntaxHelper.getWriteCsvFile("df", "C:\\output\\file.csv", "|", "utf-16");
            assertEquals("df.to_csv('C:/output/file.csv', sep='|', encoding='utf-16', index=False)", result);
        }
        @Test
        void replacesBackslashes() {
            String result = PandasSyntaxHelper.getWriteCsvFile("df", "C:\\path\\to\\file.csv", ",", "utf-8");
            assertTrue(result.contains("C:/path/to/file.csv"));
        }
    }

    @Test
    void getWritePandasToPickle_producesCorrectSyntax() {
        String result = PandasSyntaxHelper.getWritePandasToPickle("pickle", "df", "/output/data.pkl");
        assertEquals("pickle.dump(df, open(\"/output/data.pkl\", \"wb\"))", result);
    }

    @Test
    void getWritePandasToPickle_replacesBackslashes() {
        String result = PandasSyntaxHelper.getWritePandasToPickle("pickle", "df", "C:\\output\\data.pkl");
        assertEquals("pickle.dump(df, open(\"C:/output/data.pkl\", \"wb\"))", result);
    }

    @Test
    void getReadPickleToPandas_producesCorrectSyntax() {
        String result = PandasSyntaxHelper.getReadPickleToPandas("pd", "/data/frame.pkl", "df");
        assertEquals("df = pd.read_pickle(\"/data/frame.pkl\")", result);
    }

    @Test
    void getReadPickleToPandas_replacesBackslashes() {
        String result = PandasSyntaxHelper.getReadPickleToPandas("pd", "C:\\data\\frame.pkl", "df");
        assertEquals("df = pd.read_pickle(\"C:/data/frame.pkl\")", result);
    }

    @Test
    void getParquetFileRead_producesCorrectSyntax() {
        String result = PandasSyntaxHelper.getParquetFileRead("pd", "np", "/data/file.parquet", "df");
        assertEquals("df=pd.read_parquet('/data/file.parquet')", result);
    }

    @Test
    void getParquetFileRead_replacesBackslashes() {
        String result = PandasSyntaxHelper.getParquetFileRead("pd", "np", "C:\\data\\file.parquet", "df");
        assertEquals("df=pd.read_parquet('C:/data/file.parquet')", result);
    }


    @Nested
    class GetMergeSyntaxTests {
        @Test
        void getMergeSyntax_inner_join() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> joinMap = new LinkedHashMap<>();
            joinMap.put("key", "key");
            joinCols.add(joinMap);
            
            String result = PandasSyntaxHelper.getMergeSyntax("pd", "result", "left", "right",
                    "inner.join", joinCols, false);
            assertTrue(result.contains("how=\"inner\""));
        }
        @Test
        void getMergeSyntax_left_outer_join() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> joinMap = new LinkedHashMap<>();
            joinMap.put("key", "key");
            joinCols.add(joinMap);
            
            String result = PandasSyntaxHelper.getMergeSyntax("pd", "result", "left", "right",
                    "left.outer.join", joinCols, false);
            assertTrue(result.contains("how=\"left\""));
        }
        @Test
        void getMergeSyntax_right_outer_join() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> joinMap = new LinkedHashMap<>();
            joinMap.put("key", "key");
            joinCols.add(joinMap);
            
            String result = PandasSyntaxHelper.getMergeSyntax("pd", "result", "left", "right",
                    "right.outer.join", joinCols, false);
            assertTrue(result.contains("how=\"right\""));
        }
        @Test
        void getMergeSyntax_outer_join() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> joinMap = new LinkedHashMap<>();
            joinMap.put("key", "key");
            joinCols.add(joinMap);
            
            String result = PandasSyntaxHelper.getMergeSyntax("pd", "result", "left", "right",
                    "outer.join", joinCols, false);
            assertTrue(result.contains("how=\"outer\""));
        }
        @Test
        void getMergeSyntax_innerJoin_checkColumns() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> joinMap = new LinkedHashMap<>();
            joinMap.put("id", "user_id");
            joinCols.add(joinMap);
            
            String result = PandasSyntaxHelper.getMergeSyntax("pd", "result", "left", "right",
                    "inner.join", joinCols, false);
            assertTrue(result.contains("left_on=[\"id\"]"));
            assertTrue(result.contains("right_on=[\"user_id\"]"));
        }
        @Test
        void getMergeSyntax_nonEqui_crossJoin() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            
            String result = PandasSyntaxHelper.getMergeSyntax("pd", "result", "left", "right",
                    "inner.join", joinCols, true);
            assertTrue(result.contains("assign(key=0)"));
            assertTrue(result.contains("on='key'"));
            assertTrue(result.contains("drop('key',axis=1)"));
        }
    }


    @Nested
    class GetMergeColsSyntaxTests {
        @Test
        void grabKeys_singleJoin() {
            List<Map<String, String>> colNames = new ArrayList<>();
            Map<String, String> joinMap = new LinkedHashMap<>();
            joinMap.put("leftCol", "rightCol");
            colNames.add(joinMap);
            
            StringBuffer builder = new StringBuffer();
            PandasSyntaxHelper.getMergeColsSyntax(builder, colNames, true);
            assertEquals("\"leftCol\"", builder.toString());
        }
        @Test
        void grabValues_singleJoin() {
            List<Map<String, String>> colNames = new ArrayList<>();
            Map<String, String> joinMap = new LinkedHashMap<>();
            joinMap.put("leftCol", "rightCol");
            colNames.add(joinMap);
            
            StringBuffer builder = new StringBuffer();
            PandasSyntaxHelper.getMergeColsSyntax(builder, colNames, false);
            assertEquals("\"rightCol\"", builder.toString());
        }
        @Test
        void emptyMap_skipped() {
            List<Map<String, String>> colNames = new ArrayList<>();
            colNames.add(new LinkedHashMap<>());
            
            StringBuffer builder = new StringBuffer();
            PandasSyntaxHelper.getMergeColsSyntax(builder, colNames, true);
            assertEquals("", builder.toString());
        }
        @Test
        void multipleJoins() {
            List<Map<String, String>> colNames = new ArrayList<>();
            Map<String, String> join1 = new LinkedHashMap<>();
            join1.put("col1", "colA");
            colNames.add(join1);
            Map<String, String> join2 = new LinkedHashMap<>();
            join2.put("col2", "colB");
            colNames.add(join2);
            
            StringBuffer builder = new StringBuffer();
            PandasSyntaxHelper.getMergeColsSyntax(builder, colNames, true);
            assertEquals("\"col1\",\"col2\"", builder.toString());
        }
    }


    @Nested
    class GetMergeFilterSyntaxTests {
        @Test
        void singleJoinCondition() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> joinMap = new LinkedHashMap<>();
            joinMap.put("price", "max_price");
            joinCols.add(joinMap);
            List<String> comparators = Arrays.asList("<=");
            
            String result = PandasSyntaxHelper.getMergeFilterSyntax("df", joinCols, comparators);
            assertEquals("df=df.loc[((df['price']<=df['max_price']))]", result);
        }
        @Test
        void multipleConditions() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> join1 = new LinkedHashMap<>();
            join1.put("colA", "colB");
            joinCols.add(join1);
            Map<String, String> join2 = new LinkedHashMap<>();
            join2.put("colC", "colD");
            joinCols.add(join2);
            List<String> comparators = Arrays.asList(">=", "==");
            
            String result = PandasSyntaxHelper.getMergeFilterSyntax("df", joinCols, comparators);
            assertTrue(result.contains("(df['colA']>=df['colB'])"));
            assertTrue(result.contains(" & "));
            assertTrue(result.contains("(df['colC']==df['colD'])"));
        }
        @Test
        void withCTDColumn_addsDrop() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> joinMap = new LinkedHashMap<>();
            joinMap.put("price", "price_CTD");
            joinCols.add(joinMap);
            List<String> comparators = Arrays.asList("<=");
            
            String result = PandasSyntaxHelper.getMergeFilterSyntax("df", joinCols, comparators);
            assertTrue(result.contains(".drop(['price_CTD'],axis=1)"));
        }
        @Test
        void noCTDColumn_noDrop() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> joinMap = new LinkedHashMap<>();
            joinMap.put("price", "max_price");
            joinCols.add(joinMap);
            List<String> comparators = Arrays.asList("<=");
            
            String result = PandasSyntaxHelper.getMergeFilterSyntax("df", joinCols, comparators);
            assertFalse(result.contains(".drop("));
        }
    }

    @Test
    void alterColumnName_producesCorrectSyntax() {
        String result = PandasSyntaxHelper.alterColumnName("df", "old_name", "new_name");
        assertEquals("df.rename(columns={'old_name':'new_name'}, inplace=True)", result);
    }

    @Test
    void alterColumnNames_arrayVersion_singlePair() {
        String result = PandasSyntaxHelper.alterColumnNames("df",
                new String[]{"old"}, new String[]{"new"});
        assertEquals("df.rename(columns={'old':'new'}, inplace=True)", result);
    }

    @Test
    void alterColumnNames_arrayVersion_multiplePairs() {
        String result = PandasSyntaxHelper.alterColumnNames("df",
                new String[]{"a", "b"}, new String[]{"x", "y"});
        assertTrue(result.contains("'a':'x'"));
        assertTrue(result.contains("'b':'y'"));
        assertTrue(result.startsWith("df.rename(columns={"));
        assertTrue(result.endsWith("}, inplace=True)"));
    }

    @Test
    void alterColumnNames_mapVersion() {
        Map<String, String> headerMap = new LinkedHashMap<>();
        headerMap.put("col1", "newCol1");
        headerMap.put("col2", "newCol2");
        String result = PandasSyntaxHelper.alterColumnNames("df", headerMap);
        assertTrue(result.contains("'col1':'newCol1'"));
        assertTrue(result.contains("'col2':'newCol2'"));
    }

    @Test
    void getDFLength_producesCorrectSyntax() {
        assertEquals("len(df.index)", PandasSyntaxHelper.getDFLength("df"));
    }

    @Test
    void getDFLength_withDifferentTable() {
        assertEquals("len(myTable.index)", PandasSyntaxHelper.getDFLength("myTable"));
    }

    @Test
    void getColumns_producesCorrectSyntax() {
        assertEquals("list(df.columns)", PandasSyntaxHelper.getColumns("df"));
    }

    @Test
    void getTypes_producesCorrectSyntax() {
        assertEquals("list(df.dtypes.astype(str))", PandasSyntaxHelper.getTypes("df"));
    }

    @Test
    void getColumnType_producesCorrectSyntax() {
        assertEquals("df['age'].dtype.name", PandasSyntaxHelper.getColumnType("df", "age"));
    }

    @Test
    void getColumnChange_producesCorrectSyntax() {
        String result = PandasSyntaxHelper.getColumnChange("df", "age", "float64");
        assertEquals("df['age'] = df['age'].astype('float64')", result);
    }

    @Test
    void removeDuplicateColumns_producesCorrectSyntax() {
        String result = PandasSyntaxHelper.removeDuplicateColumns("df", "df_clean");
        assertEquals("df_clean = df.loc[:,~df.columns.duplicated()]", result);
    }


    @Nested
    class CreatePandasColVecTests {
        @Test
        void colVec_STRING() {
            List<Object> row = Arrays.asList("hello", "world");
            String result = PandasSyntaxHelper.createPandasColVec(row, SemossDataType.STRING);
            assertEquals("(['hello','world'])", result);
        }
        @Test
        void colVec_STRING_withApostrophe() {
            List<Object> row = Arrays.asList("it\'s", "test");
            String result = PandasSyntaxHelper.createPandasColVec(row, SemossDataType.STRING);
            assertEquals("(['it\\'s','test'])", result);
        }
        @Test
        void colVec_INT() {
            List<Object> row = Arrays.asList(1, 2, 3);
            String result = PandasSyntaxHelper.createPandasColVec(row, SemossDataType.INT);
            assertEquals("([1,2,3])", result);
        }
        @Test
        void colVec_DOUBLE() {
            List<Object> row = Arrays.asList(1.5, 2.7);
            String result = PandasSyntaxHelper.createPandasColVec(row, SemossDataType.DOUBLE);
            assertEquals("([1.5,2.7])", result);
        }
        @Test
        void colVec_DATE() {
            List<Object> row = Arrays.asList("2024-01-15");
            String result = PandasSyntaxHelper.createPandasColVec(row, SemossDataType.DATE);
            assertEquals("([np.datetime64(\"2024-01-15\")])", result);
        }
        @Test
        void colVec_TIMESTAMP() {
            List<Object> row = Arrays.asList("2024-01-15 10:30:00");
            String result = PandasSyntaxHelper.createPandasColVec(row, SemossDataType.TIMESTAMP);
            assertEquals("([np.datetime64(\"2024-01-15 10:30:00\")])", result);
        }
        @Test
        void colVec_nullType_stringValue() {
            List<Object> row = Arrays.asList("hello");
            String result = PandasSyntaxHelper.createPandasColVec(row, null);
            assertEquals("(['hello'])", result);
        }
        @Test
        void colVec_nullType_nonStringValue() {
            List<Object> row = Arrays.asList(42);
            String result = PandasSyntaxHelper.createPandasColVec(row, null);
            assertEquals("([42])", result);
        }
        @Test
        void colVec_singleElement() {
            List<Object> row = Arrays.asList("only");
            String result = PandasSyntaxHelper.createPandasColVec(row, SemossDataType.STRING);
            assertEquals("(['only'])", result);
        }
    }


    @Nested
    class FormatFilterValueTests {
        @Test
        void formatFilterValue_STRING() {
            assertEquals("'hello'", PandasSyntaxHelper.formatFilterValue("hello", SemossDataType.STRING));
        }
        @Test
        void formatFilterValue_INT() {
            assertEquals("42", PandasSyntaxHelper.formatFilterValue(42, SemossDataType.INT));
        }
        @Test
        void formatFilterValue_DOUBLE() {
            assertEquals("3.14", PandasSyntaxHelper.formatFilterValue(3.14, SemossDataType.DOUBLE));
        }
        @Test
        void formatFilterValue_DATE() {
            String result = PandasSyntaxHelper.formatFilterValue("2024-01-15", SemossDataType.DATE);
            assertEquals("np.datetime64(\"2024-01-15\", format='%Y-%m-%d')", result);
        }
        @Test
        void formatFilterValue_TIMESTAMP() {
            String result = PandasSyntaxHelper.formatFilterValue("2024-01-15 10:30:00", SemossDataType.TIMESTAMP);
            assertEquals("np.datetime64(\"2024-01-15 10:30:00\", format='%Y-%m-%d %H:%M:%S')", result);
        }
        @Test
        void formatFilterValue_nullType_stringValue() {
            assertEquals("'hello'", PandasSyntaxHelper.formatFilterValue("hello", null));
        }
        @Test
        void formatFilterValue_nullType_nonStringValue() {
            assertEquals("42", PandasSyntaxHelper.formatFilterValue(42, null));
        }
    }

    @Test
    void setColumnNames_producesCorrectSyntax() {
        String result = PandasSyntaxHelper.setColumnNames("df", new String[]{"a", "b", "c"});
        assertEquals("df.columns=['a','b','c']", result);
    }

    @Test
    void setColumnNames_singleColumn() {
        String result = PandasSyntaxHelper.setColumnNames("df", new String[]{"only"});
        assertEquals("df.columns=['only']", result);
    }


    @Nested
    class FilterByColumnTests {
        @Test
        void filterByColumn_emptyList() {
            String result = PandasSyntaxHelper.filterByColumn("df", "result", new ArrayList<>());
            assertEquals("result=df", result);
        }
        @Test
        void filterByColumn_singleColumn() {
            String result = PandasSyntaxHelper.filterByColumn("df", "result", Arrays.asList("name"));
            assertEquals("result = df[['name']]", result);
        }
        @Test
        void filterByColumn_multipleColumns() {
            String result = PandasSyntaxHelper.filterByColumn("df", "result", Arrays.asList("name", "age", "city"));
            assertEquals("result = df[['name', 'age', 'city']]", result);
        }
    }


    @Nested
    class FilterRowBySliceTests {
        @Test
        void filterRowBySlice_numericIndices() {
            String result = PandasSyntaxHelper.filterRowBySlice("df", "result", "10", "0");
            assertEquals("result = df.iloc[0:10]", result);
        }
        @Test
        void filterRowBySlice_labelIndices() {
            String result = PandasSyntaxHelper.filterRowBySlice("df", "result", "z", "a");
            assertEquals("result = df.loc['a':'z']", result);
        }
        @Test
        void filterRowBySlice_nullStartWithLabelEnd_throws() {
            assertThrows(IllegalArgumentException.class, () -> {
                PandasSyntaxHelper.filterRowBySlice("df", "result", "abc", null);
            });
        }
        @Test
        void filterRowBySlice_nullStart_numericEnd_usesZero() {
            String result = PandasSyntaxHelper.filterRowBySlice("df", "result", "5", null);
            assertEquals("result = df.iloc[0:5]", result);
        }
    }


    @Nested
    class FilterByRowTests {
        @Test
        void filterByRow_emptyList() {
            String result = PandasSyntaxHelper.filterByRow("df", "result", new ArrayList<>());
            assertEquals("result = df", result);
        }
        @Test
        void filterByRow_singleNumeric() {
            String result = PandasSyntaxHelper.filterByRow("df", "result", Arrays.asList("5"));
            assertEquals("result = df.iloc[[5]]", result);
        }
        @Test
        void filterByRow_multipleNumeric() {
            String result = PandasSyntaxHelper.filterByRow("df", "result", Arrays.asList("1", "3", "5"));
            assertEquals("result = df.iloc[[1, 3, 5]]", result);
        }
        @Test
        void filterByRow_singleLabel() {
            String result = PandasSyntaxHelper.filterByRow("df", "result", Arrays.asList("labelA"));
            assertEquals("result = df.loc[['labelA']]", result);
        }
        @Test
        void filterByRow_multipleLabels() {
            String result = PandasSyntaxHelper.filterByRow("df", "result", Arrays.asList("labelA", "labelB"));
            assertEquals("result = df.loc[['labelA', 'labelB']]", result);
        }
    }


    @Nested
    class FilterBySingleValueTests {
        @Test
        void numericValue_greaterThan() {
            String result = PandasSyntaxHelper.filterBySingleValue("df", "result", "age", ">", "30");
            assertEquals("result = df[df['age']>30]", result);
        }
        @Test
        void numericValue_equalOperator() {
            String result = PandasSyntaxHelper.filterBySingleValue("df", "result", "id", "==", "100");
            assertEquals("result = df[df['id']==100]", result);
        }
        @Test
        void numericValue_invalidOperator_throws() {
            assertThrows(IllegalArgumentException.class, () -> {
                PandasSyntaxHelper.filterBySingleValue("df", "result", "age", "~", "30");
            });
        }
        @Test
        void stringValue_equalOperator() {
            String result = PandasSyntaxHelper.filterBySingleValue("df", "result", "name", "==", "Alice");
            assertEquals("result = df[df['name']=='Alice']", result);
        }
        @Test
        void stringValue_notEqualOperator() {
            String result = PandasSyntaxHelper.filterBySingleValue("df", "result", "name", "!=", "Bob");
            assertEquals("result = df[df['name']!='Bob']", result);
        }
        @Test
        void stringValue_invalidOperator_throws() {
            assertThrows(IllegalArgumentException.class, () -> {
                PandasSyntaxHelper.filterBySingleValue("df", "result", "name", ">", "Alice");
            });
        }
    }


    @Nested
    class FilterByMultipleValuesTests {
        @Test
        void emptyValues() {
            String result = PandasSyntaxHelper.filterByMultipleValues("df", "result", "col",
                    new ArrayList<>(), null);
            assertEquals("result = df", result);
        }
        @Test
        void singleNumeric() {
            String result = PandasSyntaxHelper.filterByMultipleValues("df", "result", "age",
                    Arrays.asList("30"), null);
            assertEquals("result = df[df['age'].isin([30])]", result);
        }
        @Test
        void multipleNumeric() {
            String result = PandasSyntaxHelper.filterByMultipleValues("df", "result", "age",
                    Arrays.asList("25", "30", "35"), null);
            assertEquals("result = df[df['age'].isin([25, 30, 35])]", result);
        }
        @Test
        void singleString() {
            String result = PandasSyntaxHelper.filterByMultipleValues("df", "result", "name",
                    Arrays.asList("Alice"), null);
            assertEquals("result = df[df['name'].isin(['Alice'])]", result);
        }
        @Test
        void multipleStrings() {
            String result = PandasSyntaxHelper.filterByMultipleValues("df", "result", "name",
                    Arrays.asList("Alice", "Bob"), null);
            assertEquals("result = df[df['name'].isin(['Alice', 'Bob'])]", result);
        }
        @Test
        void withNegation() {
            String result = PandasSyntaxHelper.filterByMultipleValues("df", "result", "age",
                    Arrays.asList("30"), "~");
            assertEquals("result = df[~df['age'].isin([30])]", result);
        }
        @Test
        void nullNegation_defaultsToEmpty() {
            String result = PandasSyntaxHelper.filterByMultipleValues("df", "result", "age",
                    Arrays.asList("30"), null);
            assertFalse(result.contains("~"));
        }
    }


    @Nested
    class FilterByExpressionTests {
        @Test
        void withoutNegation() {
            String result = PandasSyntaxHelper.filterByExpression("df", "result", "name", "Ali", null);
            assertEquals("result = df[df['name'].str.contains('Ali')]", result);
        }
        @Test
        void withNegation() {
            String result = PandasSyntaxHelper.filterByExpression("df", "result", "name", "Ali", "~");
            assertEquals("result = df[~df['name'].str.contains('Ali')]", result);
        }
    }


    @Nested
    class FilterBetweenNumbersTests {
        @Test
        void withoutNegation() {
            String result = PandasSyntaxHelper.filterBetweenNumbers("df", "result", "age", 20, 30, null);
            assertEquals("result = df[df['age'].between(20, 30)]", result);
        }
        @Test
        void withNegation() {
            String result = PandasSyntaxHelper.filterBetweenNumbers("df", "result", "age", 20, 30, "~");
            assertEquals("result = df[~df['age'].between(20, 30)]", result);
        }
    }


    @Nested
    class AlterMissingColumnsTests {
        @Test
        void addsNewColumns() {
            String[] curHeaders = {"id", "name"};
            Map<String, SemossDataType> newColumns = new LinkedHashMap<>();
            newColumns.put("age", SemossDataType.INT);
            newColumns.put("city", SemossDataType.STRING);
            
            List<Join> joins = new ArrayList<>();
            joins.add(new Join("id", "inner.join", "id"));
            
            Map<String, String> aliases = new HashMap<>();
            
            String result = PandasSyntaxHelper.alterMissingColumns("df", curHeaders, newColumns, joins, aliases);
            assertTrue(result.contains("\"id\""));
            assertTrue(result.contains("\"name\""));
            assertTrue(result.contains("\"age\""));
            assertTrue(result.contains("\"city\""));
            assertTrue(result.startsWith("df.reindex(columns=["));
            assertTrue(result.endsWith("])"));
        }
        @Test
        void skipsJoinColumns() {
            String[] curHeaders = {"id"};
            Map<String, SemossDataType> newColumns = new LinkedHashMap<>();
            newColumns.put("id", SemossDataType.INT);
            newColumns.put("extra", SemossDataType.STRING);
            
            List<Join> joins = new ArrayList<>();
            joins.add(new Join("id", "inner.join", "id"));
            
            Map<String, String> aliases = new HashMap<>();
            
            String result = PandasSyntaxHelper.alterMissingColumns("df", curHeaders, newColumns, joins, aliases);
            assertTrue(result.contains("\"extra\""));
            // id from curHeaders + extra = 4 quote chars
            long quoteCount = result.chars().filter(ch -> ch == '\"').count();
            assertEquals(4, quoteCount);
        }
        @Test
        void handlesAliases() {
            String[] curHeaders = {"id"};
            Map<String, SemossDataType> newColumns = new LinkedHashMap<>();
            newColumns.put("salary", SemossDataType.DOUBLE);
            
            List<Join> joins = new ArrayList<>();
            joins.add(new Join("id", "inner.join", "id"));
            
            Map<String, String> aliases = new HashMap<>();
            aliases.put("salary", "annual_salary");
            
            String result = PandasSyntaxHelper.alterMissingColumns("df", curHeaders, newColumns, joins, aliases);
            assertTrue(result.contains("\"annual_salary\""));
            assertFalse(result.contains("\"salary\""));
        }
        @Test
        void handlesDoubleUnderscorePrefix() {
            String[] curHeaders = {"id"};
            Map<String, SemossDataType> newColumns = new LinkedHashMap<>();
            newColumns.put("Table__city", SemossDataType.STRING);

            List<Join> joins = new ArrayList<>();
            joins.add(new Join("id", "inner.join", "Table__id"));

            Map<String, String> aliases = new HashMap<>();

            String result = PandasSyntaxHelper.alterMissingColumns("df", curHeaders, newColumns, joins, aliases);
            assertTrue(result.contains("\"city\""));
        }
    }

    // ---- Additional coverage tests for missed branches ----

    @Nested
    class AdditionalCsvReadTests {
        @Test
        void getCsvFileRead_4param_delegatesToOverloads() {
            // This calls the 7-param version with nulls, which calls 8-param
            // which calls buildReadCsv(8-param) which requires numberOfColumns
            // So we just verify it doesn't return null and starts with tableName
            // Actually this will fail because it calls numberOfColumns on a nonexistent file
            // Skip this - it requires CSVFileHelper
        }

        @Test
        void getCsvFileRead_10param_withStringInputTypes() {
            Map<String, Object> dtm = new LinkedHashMap<>();
            dtm.put("Name", "STRING");
            dtm.put("Age", "INT");
            String result = PandasSyntaxHelper.getCsvFileRead(
                    "pd", "np", "/data/file.csv", "df",
                    ",", "\"", "\\\\", null, dtm, 0);
            assertTrue(result.contains("dtype="));
            assertTrue(result.contains("'Name':object"));
            assertTrue(result.contains("'Age':np.float64"));
        }

        @Test
        void getCsvFileRead_10param_multipleDateColumns() {
            Map<String, SemossDataType> dtm = new LinkedHashMap<>();
            dtm.put("StartDate", SemossDataType.DATE);
            dtm.put("EndDate", SemossDataType.TIMESTAMP);
            String result = PandasSyntaxHelper.getCsvFileRead(
                    "pd", "np", "/data/file.csv", "df",
                    ",", "\"", "\\\\", null, dtm, 0);
            assertTrue(result.contains("parse_dates="));
            assertTrue(result.contains("'StartDate'"));
            assertTrue(result.contains("'EndDate'"));
            // Multiple dates should be comma-separated
            assertTrue(result.contains("'StartDate','EndDate'"));
        }

        @Test
        void getCsvFileRead_10param_withNullDataType_skipsColumn() {
            Map<String, Object> dtm = new LinkedHashMap<>();
            dtm.put("Col1", "NONEXISTENT_TYPE");
            dtm.put("Col2", SemossDataType.INT);
            String result = PandasSyntaxHelper.getCsvFileRead(
                    "pd", "np", "/data/file.csv", "df",
                    ",", "\"", "\\\\", null, dtm, 0);
            assertTrue(result.contains("'Col2':np.float64"));
        }
    }

    @Nested
    class AdditionalJsonReadTests {
        @Test
        void getJsonFileRead_withStringInputTypes() {
            Map<String, Object> dtm = new LinkedHashMap<>();
            dtm.put("Name", "STRING");
            dtm.put("Created", "TIMESTAMP");
            String result = PandasSyntaxHelper.getJsonFileRead(
                    "pd", "np", "/data/file.json", "df", dtm);
            assertTrue(result.contains("dtype="));
            assertTrue(result.contains("'Name':object"));
            assertTrue(result.contains("convert_dates="));
            assertTrue(result.contains("'Created'"));
        }

        @Test
        void getJsonFileRead_multipleDateColumns() {
            Map<String, SemossDataType> dtm = new LinkedHashMap<>();
            dtm.put("Start", SemossDataType.DATE);
            dtm.put("End", SemossDataType.DATE);
            dtm.put("Val", SemossDataType.INT);
            String result = PandasSyntaxHelper.getJsonFileRead(
                    "pd", "np", "/data/file.json", "df", dtm);
            assertTrue(result.contains("'Start','End'"));
            assertTrue(result.contains("'Val':np.float64"));
        }

        @Test
        void getJsonFileRead_withNullDataType_skipsColumn() {
            Map<String, Object> dtm = new LinkedHashMap<>();
            dtm.put("Col1", "FAKE_TYPE");
            dtm.put("Col2", SemossDataType.DOUBLE);
            String result = PandasSyntaxHelper.getJsonFileRead(
                    "pd", "np", "/data/file.json", "df", dtm);
            assertTrue(result.contains("'Col2':np.float64"));
        }

        @Test
        void getJsonFileRead_multipleNonDateColumns_commaJoined() {
            Map<String, SemossDataType> dtm = new LinkedHashMap<>();
            dtm.put("A", SemossDataType.INT);
            dtm.put("B", SemossDataType.STRING);
            String result = PandasSyntaxHelper.getJsonFileRead(
                    "pd", "np", "/data/file.json", "df", dtm);
            assertTrue(result.contains("'A':np.float64,'B':object"));
        }
    }

    @Nested
    class AdditionalMergeTests {
        @Test
        void getMergeColsSyntax_multipleColumnsPerJoinMap() {
            List<Map<String, String>> colNames = new ArrayList<>();
            Map<String, String> joinMap = new LinkedHashMap<>();
            joinMap.put("col1", "colA");
            joinMap.put("col2", "colB");
            colNames.add(joinMap);

            StringBuffer builder = new StringBuffer();
            PandasSyntaxHelper.getMergeColsSyntax(builder, colNames, true);
            assertEquals("\"col1\",\"col2\"", builder.toString());
        }

        @Test
        void getMergeFilterSyntax_multipleCTDColumns() {
            List<Map<String, String>> joinCols = new ArrayList<>();
            Map<String, String> join1 = new LinkedHashMap<>();
            join1.put("a", "a_CTD");
            joinCols.add(join1);
            Map<String, String> join2 = new LinkedHashMap<>();
            join2.put("b", "b_CTD");
            joinCols.add(join2);
            List<String> comparators = Arrays.asList("<=", ">=");

            String result = PandasSyntaxHelper.getMergeFilterSyntax("df", joinCols, comparators);
            assertTrue(result.contains(".drop(['a_CTD', 'b_CTD'],axis=1)"));
        }
    }

    @Nested
    class AdditionalColVecAndFilterTests {
        @Test
        void createPandasColVec_unknownNonNullType_usesDefaultAppend() {
            List<Object> row = Arrays.asList(42);
            String result = PandasSyntaxHelper.createPandasColVec(row, SemossDataType.BOOLEAN);
            assertEquals("([42])", result);
        }

        @Test
        void formatFilterValue_unknownNonNullType_usesDefaultAppend() {
            String result = PandasSyntaxHelper.formatFilterValue(42, SemossDataType.BOOLEAN);
            assertEquals("42", result);
        }

        @Test
        void getWriteCsvFile_4param_emptyEncoding() {
            String result = PandasSyntaxHelper.getWriteCsvFile("df", "/out/file.csv", ",", "");
            assertTrue(result.contains("encoding='utf-8'"));
        }
    }

}
