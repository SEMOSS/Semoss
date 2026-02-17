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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.util.Utility;

class RIteratorUnitTests {

    private MockedStatic<Utility> utilityMock;
    private RFrameBuilder mockBuilder;
    private AbstractRJavaTranslator mockTranslator;

    @BeforeEach
    void setUp() {
        utilityMock = mockStatic(Utility.class);
        utilityMock.when(() -> Utility.getRandomString(6)).thenReturn("abc123");

        mockTranslator = mock(AbstractRJavaTranslator.class);
        mockBuilder = mock(RFrameBuilder.class);
        when(mockBuilder.getRJavaTranslator()).thenReturn(mockTranslator);
    }

    @AfterEach
    void tearDown() {
        utilityMock.close();
    }

    private RIterator createIterator(String rQuery, int numRows, String[] headers, String[] colTypes) {
        when(mockBuilder.getNumRows("tempabc123")).thenReturn(numRows);
        when(mockBuilder.getColumnNames("tempabc123")).thenReturn(headers);
        when(mockBuilder.getColumnTypes("tempabc123[," + RSyntaxHelper.createStringRColVec(headers) + "]"))
                .thenReturn(colTypes);
        return new RIterator(mockBuilder, rQuery);
    }

    private RIterator createIteratorWithQs(String rQuery, int numRows, SelectQueryStruct qs) {
        when(mockBuilder.getNumRows("tempabc123")).thenReturn(numRows);
        // headers will come from qs.getHeaderInfo()
        String[] headers = {"col1"};
        when(mockBuilder.getColumnTypes(anyString())).thenReturn(new String[]{"character"});
        return new RIterator(mockBuilder, rQuery, qs);
    }

    @Nested
    class ConstructorWithoutQsTests {
        @Test
        void constructor_callsEvalRWithTempVarQuery() {
            createIterator("datatable", 0, new String[]{"col1"}, new String[]{"character"});
            verify(mockBuilder).evalR("tempabc123 <- {datatable}");
        }

        @Test
        void constructor_queriesNumRows() {
            createIterator("datatable", 10, new String[]{"col1"}, new String[]{"character"});
            verify(mockBuilder).getNumRows("tempabc123");
        }

        @Test
        void constructor_queriesColumnNames() {
            createIterator("datatable", 5, new String[]{"col1", "col2"}, new String[]{"character", "integer"});
            verify(mockBuilder).getColumnNames("tempabc123");
        }

        @Test
        void constructor_queriesColumnTypes() {
            String[] headers = {"col1"};
            createIterator("datatable", 5, headers, new String[]{"character"});
            verify(mockBuilder).getColumnTypes("tempabc123[," + RSyntaxHelper.createStringRColVec(headers) + "]");
        }
    }

    @Nested
    class ConstructorWithQsTests {
        @Test
        void constructorWithQs_usesHeaderInfoFromQs() {
            SelectQueryStruct qs = mock(SelectQueryStruct.class);
            List<Map<String, Object>> headerInfo = new ArrayList<>();
            Map<String, Object> h1 = new HashMap<>();
            h1.put("alias", "Title");
            headerInfo.add(h1);
            Map<String, Object> h2 = new HashMap<>();
            h2.put("alias", "Genre");
            headerInfo.add(h2);
            when(qs.getHeaderInfo()).thenReturn(headerInfo);
            when(qs.getLimit()).thenReturn(-1L);
            when(qs.getOffset()).thenReturn(0L);
            when(mockBuilder.getNumRows("tempabc123")).thenReturn(5);
            when(mockBuilder.getColumnTypes(anyString())).thenReturn(new String[]{"character", "character"});

            RIterator it = new RIterator(mockBuilder, "query", qs);

            assertArrayEquals(new String[]{"Title", "Genre"}, it.getHeaders());
        }

        @Test
        void constructorWithHardSelectQueryStruct_usesBuilderColumnNames() {
            HardSelectQueryStruct qs = mock(HardSelectQueryStruct.class);
            when(qs.getLimit()).thenReturn(-1L);
            when(qs.getOffset()).thenReturn(0L);
            when(mockBuilder.getNumRows("tempabc123")).thenReturn(3);
            when(mockBuilder.getColumnNames("tempabc123")).thenReturn(new String[]{"a", "b"});
            when(mockBuilder.getColumnTypes(anyString())).thenReturn(new String[]{"integer", "character"});

            RIterator it = new RIterator(mockBuilder, "query", qs);

            assertArrayEquals(new String[]{"a", "b"}, it.getHeaders());
        }

        @Test
        void constructorWithQs_offsetExceedsRows_setsNumRowsToZero() {
            SelectQueryStruct qs = mock(SelectQueryStruct.class);
            List<Map<String, Object>> headerInfo = new ArrayList<>();
            Map<String, Object> h1 = new HashMap<>();
            h1.put("alias", "col1");
            headerInfo.add(h1);
            when(qs.getHeaderInfo()).thenReturn(headerInfo);
            when(qs.getLimit()).thenReturn(-1L);
            when(qs.getOffset()).thenReturn(100L);
            when(mockBuilder.getNumRows("tempabc123")).thenReturn(5);
            when(mockBuilder.getColumnTypes(anyString())).thenReturn(new String[]{"character"});

            RIterator it = new RIterator(mockBuilder, "query", qs);

            assertFalse(it.hasNext());
        }

        @Test
        void constructorWithQs_withLimitAndOffset_callsDetermineLimitOffsetSyntax() {
            SelectQueryStruct qs = mock(SelectQueryStruct.class);
            List<Map<String, Object>> headerInfo = new ArrayList<>();
            Map<String, Object> h1 = new HashMap<>();
            h1.put("alias", "col1");
            headerInfo.add(h1);
            when(qs.getHeaderInfo()).thenReturn(headerInfo);
            when(qs.getLimit()).thenReturn(10L);
            when(qs.getOffset()).thenReturn(0L);
            when(mockBuilder.getNumRows("tempabc123")).thenReturn(50).thenReturn(10);
            when(mockBuilder.getColumnTypes(anyString())).thenReturn(new String[]{"character"});

            RIterator it = new RIterator(mockBuilder, "query", qs);

            // Should have called evalR with the limit/offset syntax
            verify(mockBuilder, atLeast(2)).evalR(anyString());
        }
    }

    @Nested
    class HasNextTests {
        @Test
        void hasNext_true_whenRowsRemain() {
            RIterator it = createIterator("dt", 5, new String[]{"col1"}, new String[]{"character"});
            assertTrue(it.hasNext());
        }

        @Test
        void hasNext_false_whenNoRows() {
            RIterator it = createIterator("dt", 0, new String[]{"col1"}, new String[]{"character"});
            assertFalse(it.hasNext());
        }

        @Test
        void hasNext_false_callsCleanUp() {
            RIterator it = createIterator("dt", 0, new String[]{"col1"}, new String[]{"character"});
            it.hasNext();
            // cleanUp calls evalR with rm(tempVar);gc();
            verify(mockBuilder, atLeast(2)).evalR(anyString());
        }
    }

    @Nested
    class NextTests {
        @Test
        void next_singleRow_callsGetDataRow() {
            String[] headers = {"col1"};
            when(mockBuilder.getNumRows("tempabc123")).thenReturn(1);
            when(mockBuilder.getColumnNames("tempabc123")).thenReturn(headers);
            when(mockBuilder.getColumnTypes(anyString())).thenReturn(new String[]{"character"});
            when(mockBuilder.getDataRow("tempabc123[1]", headers)).thenReturn(new Object[]{"value1"});

            RIterator it = new RIterator(mockBuilder, "dt");

            IHeadersDataRow row = it.next();
            assertArrayEquals(headers, row.getHeaders());
            assertArrayEquals(new Object[]{"value1"}, row.getValues());
        }

        @Test
        void next_multipleRows_callsGetBulkDataRow() {
            String[] headers = {"col1"};
            when(mockBuilder.getNumRows("tempabc123")).thenReturn(3);
            when(mockBuilder.getColumnNames("tempabc123")).thenReturn(headers);
            when(mockBuilder.getColumnTypes(anyString())).thenReturn(new String[]{"character"});

            List<Object[]> bulkData = new ArrayList<>();
            bulkData.add(new Object[]{"val1"});
            bulkData.add(new Object[]{"val2"});
            bulkData.add(new Object[]{"val3"});
            when(mockBuilder.getBulkDataRow("tempabc123[1:3]", headers)).thenReturn(bulkData);

            RIterator it = new RIterator(mockBuilder, "dt");

            IHeadersDataRow row1 = it.next();
            assertArrayEquals(new Object[]{"val1"}, row1.getValues());

            IHeadersDataRow row2 = it.next();
            assertArrayEquals(new Object[]{"val2"}, row2.getValues());

            IHeadersDataRow row3 = it.next();
            assertArrayEquals(new Object[]{"val3"}, row3.getValues());
        }

        @Test
        void next_afterAllRowsConsumed_hasNextReturnsFalse() {
            String[] headers = {"col1"};
            when(mockBuilder.getNumRows("tempabc123")).thenReturn(1);
            when(mockBuilder.getColumnNames("tempabc123")).thenReturn(headers);
            when(mockBuilder.getColumnTypes(anyString())).thenReturn(new String[]{"character"});
            when(mockBuilder.getDataRow("tempabc123[1]", headers)).thenReturn(new Object[]{"value1"});

            RIterator it = new RIterator(mockBuilder, "dt");

            it.next();
            assertFalse(it.hasNext());
        }
    }

    @Nested
    class GetSetTests {
        @Test
        void getHeaders_returnsInitializedHeaders() {
            String[] headers = {"col1", "col2"};
            RIterator it = createIterator("dt", 5, headers, new String[]{"character", "integer"});
            assertArrayEquals(headers, it.getHeaders());
        }

        @Test
        void setHeaders_overridesHeaders() {
            RIterator it = createIterator("dt", 5, new String[]{"col1"}, new String[]{"character"});
            String[] newHeaders = {"newCol"};
            it.setHeaders(newHeaders);
            assertArrayEquals(newHeaders, it.getHeaders());
        }

        @Test
        void getColTypes_returnsInitializedTypes() {
            String[] types = {"character", "integer"};
            RIterator it = createIterator("dt", 5, new String[]{"col1", "col2"}, types);
            assertArrayEquals(types, it.getColTypes());
        }

        @Test
        void setColTypes_overridesTypes() {
            RIterator it = createIterator("dt", 5, new String[]{"col1"}, new String[]{"character"});
            String[] newTypes = {"numeric"};
            it.setColTypes(newTypes);
            assertArrayEquals(newTypes, it.getColTypes());
        }

        @Test
        void getTotalNumRows_returnsOriginalRowCount() {
            RIterator it = createIterator("dt", 42, new String[]{"col1"}, new String[]{"character"});
            assertEquals(42, it.getTotalNumRows());
        }

        @Test
        void getQs_nullWhenNotProvided() {
            RIterator it = createIterator("dt", 5, new String[]{"col1"}, new String[]{"character"});
            assertNull(it.getQs());
        }

        @Test
        void getTempVarName_returnsGeneratedName() {
            RIterator it = createIterator("dt", 5, new String[]{"col1"}, new String[]{"character"});
            assertEquals("tempabc123", it.getTempVarName());
        }

        @Test
        void getQuery_nullByDefault() {
            RIterator it = createIterator("dt", 0, new String[]{"col1"}, new String[]{"character"});
            assertNull(it.getQuery());
        }

        @Test
        void setQuery_setsQuery() {
            RIterator it = createIterator("dt", 0, new String[]{"col1"}, new String[]{"character"});
            it.setQuery("myQuery");
            assertEquals("myQuery", it.getQuery());
        }
    }

    @Nested
    class SetConvertedDatesTests {
        @Test
        void setConvertedDates_null_doesNotThrow() {
            RIterator it = createIterator("dt", 0, new String[]{"col1"}, new String[]{"character"});
            assertDoesNotThrow(() -> it.setConvertedDates(null));
        }

        @Test
        void setConvertedDates_empty_doesNotModifyTypes() {
            String[] types = {"character"};
            RIterator it = createIterator("dt", 0, new String[]{"col1"}, types);
            it.setConvertedDates(new HashMap<>());
            assertArrayEquals(new String[]{"character"}, it.getColTypes());
        }

        @Test
        void setConvertedDates_updatesMatchingColumnTypes() {
            String[] headers = {"dateCol", "strCol"};
            String[] types = {"character", "character"};
            RIterator it = createIterator("dt", 0, headers, types);

            Map<String, SemossDataType> converted = new HashMap<>();
            converted.put("dateCol", SemossDataType.DATE);
            it.setConvertedDates(converted);

            assertEquals("DATE", it.getColTypes()[0]);
            assertEquals("character", it.getColTypes()[1]);
        }

        @Test
        void setConvertedDates_noMatch_doesNotModify() {
            String[] headers = {"col1"};
            String[] types = {"character"};
            RIterator it = createIterator("dt", 0, headers, types);

            Map<String, SemossDataType> converted = new HashMap<>();
            converted.put("otherCol", SemossDataType.DATE);
            it.setConvertedDates(converted);

            assertEquals("character", it.getColTypes()[0]);
        }
    }

    @Nested
    class AddVarForCleanupTests {
        @Test
        void addVarForCleanup_includesVarInCleanUp() {
            RIterator it = createIterator("dt", 0, new String[]{"col1"}, new String[]{"character"});
            it.addVarForCleanup("extraVar");
            it.cleanUp();

            // cleanUp should include both tempabc123 and extraVar
            verify(mockBuilder, atLeastOnce()).evalR(argThat(s ->
                    s.contains("rm(tempabc123)") && s.contains("rm(extraVar)") && s.contains("gc();")));
        }
    }

    @Nested
    class CleanUpTests {
        @Test
        void cleanUp_removeTempVarAndCallGc() {
            RIterator it = createIterator("dt", 0, new String[]{"col1"}, new String[]{"character"});
            it.cleanUp();

            verify(mockBuilder, atLeastOnce()).evalR(argThat(s ->
                    s.contains("rm(tempabc123)") && s.contains("gc();")));
        }

        @Test
        void cleanUp_multipleVars_removesAll() {
            RIterator it = createIterator("dt", 0, new String[]{"col1"}, new String[]{"character"});
            it.addVarForCleanup("var1");
            it.addVarForCleanup("var2");
            it.cleanUp();

            verify(mockBuilder, atLeastOnce()).evalR(argThat(s ->
                    s.contains("rm(tempabc123)") && s.contains("rm(var1)")
                            && s.contains("rm(var2)") && s.contains("gc();")));
        }
    }

    @Nested
    class GetJsonOfResultsTests {
        @Test
        void getJsonOfResults_callsTranslatorGetString() {
            String[] headers = {"col1"};
            RIterator it = createIterator("dt", 5, headers, new String[]{"character"});
            when(mockTranslator.getString(anyString())).thenReturn("{\"values\":[]}");

            String json = it.getJsonOfResults();

            assertEquals("{\"values\":[]}", json);
            verify(mockTranslator).getString(argThat(s ->
                    s.contains("jsonlite:::toJSON") && s.contains("tempabc123")));
        }
    }
}
