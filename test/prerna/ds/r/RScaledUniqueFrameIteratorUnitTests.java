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
import java.util.NoSuchElementException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.util.Utility;

class RScaledUniqueFrameIteratorUnitTests {

    private RDataTable frame;
    private RFrameBuilder builder;
    private OwlTemporalEngineMeta metaData;
    private GenRowFilters frameFilters;

    @BeforeEach
    void setUp() {
        frame = mock(RDataTable.class);
        builder = mock(RFrameBuilder.class);
        metaData = mock(OwlTemporalEngineMeta.class);
        frameFilters = mock(GenRowFilters.class);

        when(frame.getMetaData()).thenReturn(metaData);
        when(frame.getName()).thenReturn("testFrame");
        when(frame.getFrameFilters()).thenReturn(frameFilters);
        when(metaData.getHeaderToTypeMap()).thenReturn(new HashMap<>());
    }

    private RScaledUniqueFrameIterator createIterator(
            String columnName,
            String aliasResult,
            Object[] columnValues,
            Double[] maxArr,
            Double[] minArr,
            List<SemossDataType> dataTypes,
            List<String> selectors,
            String[] returnHeaders) {

        when(metaData.getUniqueNameFromAlias(columnName)).thenReturn(aliasResult);

        String effectiveUniqueName = aliasResult != null ? aliasResult : columnName;
        when(frame.getColumn(effectiveUniqueName)).thenReturn(columnValues);

        when(builder.getColumnNames("tempabc123")).thenReturn(returnHeaders);

        RScaledUniqueFrameIterator[] result = new RScaledUniqueFrameIterator[1];

        try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
             MockedStatic<QSAliasToPhysicalConverter> converterMock = mockStatic(QSAliasToPhysicalConverter.class);
             MockedConstruction<RInterpreter> interpMock = mockConstruction(RInterpreter.class,
                     (mock, context) -> {
                         when(mock.composeQuery()).thenReturn("testFrame[, c(\"col1\")]");
                     })) {

            utilMock.when(() -> Utility.getRandomString(6)).thenReturn("abc123");
            converterMock.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
                    .thenAnswer(inv -> inv.getArgument(0));

            result[0] = new RScaledUniqueFrameIterator(
                    frame, builder, columnName, maxArr, minArr, dataTypes, selectors);
        }

        return result[0];
    }

    private RScaledUniqueFrameIterator createDefaultIterator() {
        List<String> selectors = new ArrayList<>();
        selectors.add("Table__Col");

        List<SemossDataType> dataTypes = new ArrayList<>();
        dataTypes.add(SemossDataType.STRING);

        return createIterator(
                "Column",
                "Table__Column",
                new Object[]{"val1", "val2"},
                new Double[]{null},
                new Double[]{null},
                dataTypes,
                selectors,
                new String[]{"col1"});
    }

    // =========================================================================
    // Constructor Tests
    // =========================================================================

    @Nested
    class ConstructorTests {

        @Test
        void constructor_resolvesAliasFromMetaData() {
            when(metaData.getUniqueNameFromAlias("Column")).thenReturn("Table__Column");
            when(frame.getColumn("Table__Column")).thenReturn(new Object[]{"val1"});
            when(builder.getColumnNames("tempabc123")).thenReturn(new String[]{"col1"});

            List<String> selectors = new ArrayList<>();
            selectors.add("Table__Col");

            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
                 MockedStatic<QSAliasToPhysicalConverter> converterMock = mockStatic(QSAliasToPhysicalConverter.class);
                 MockedConstruction<RInterpreter> interpMock = mockConstruction(RInterpreter.class,
                         (mock, context) -> {
                             when(mock.composeQuery()).thenReturn("testFrame[, c(\"col1\")]");
                         })) {

                utilMock.when(() -> Utility.getRandomString(6)).thenReturn("abc123");
                converterMock.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
                        .thenAnswer(inv -> inv.getArgument(0));

                List<SemossDataType> dataTypes = new ArrayList<>();
                dataTypes.add(SemossDataType.STRING);

                RScaledUniqueFrameIterator iterator = new RScaledUniqueFrameIterator(
                        frame, builder, "Column",
                        new Double[]{null}, new Double[]{null},
                        dataTypes, selectors);

                verify(metaData).getUniqueNameFromAlias("Column");
                verify(frame).getColumn("Table__Column");

                when(builder.getBulkDataRow(anyString(), any())).thenReturn(new ArrayList<>());
                List<Object[]> result = iterator.next();
                verify(builder).getBulkDataRow(
                        eq("tempabc123[Column == \"val1\", ]"),
                        any(String[].class));
            }
        }

        @Test
        void constructor_usesColumnNameDirectly_whenAliasIsNull() {
            when(metaData.getUniqueNameFromAlias("Table__Column")).thenReturn(null);
            when(frame.getColumn("Table__Column")).thenReturn(new Object[]{"val1"});
            when(builder.getColumnNames("tempabc123")).thenReturn(new String[]{"col1"});

            List<String> selectors = new ArrayList<>();
            selectors.add("Table__Col");

            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
                 MockedStatic<QSAliasToPhysicalConverter> converterMock = mockStatic(QSAliasToPhysicalConverter.class);
                 MockedConstruction<RInterpreter> interpMock = mockConstruction(RInterpreter.class,
                         (mock, context) -> {
                             when(mock.composeQuery()).thenReturn("testFrame[, c(\"col1\")]");
                         })) {

                utilMock.when(() -> Utility.getRandomString(6)).thenReturn("abc123");
                converterMock.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
                        .thenAnswer(inv -> inv.getArgument(0));

                List<SemossDataType> dataTypes = new ArrayList<>();
                dataTypes.add(SemossDataType.STRING);

                RScaledUniqueFrameIterator iterator = new RScaledUniqueFrameIterator(
                        frame, builder, "Table__Column",
                        new Double[]{null}, new Double[]{null},
                        dataTypes, selectors);

                verify(frame).getColumn("Table__Column");

                when(builder.getBulkDataRow(anyString(), any())).thenReturn(new ArrayList<>());
                iterator.next();
                verify(builder).getBulkDataRow(
                        eq("tempabc123[Column == \"val1\", ]"),
                        any(String[].class));
            }
        }

        @Test
        void constructor_evaluatesRQuery() {
            when(metaData.getUniqueNameFromAlias("Column")).thenReturn("Table__Column");
            when(frame.getColumn("Table__Column")).thenReturn(new Object[]{"val1"});
            when(builder.getColumnNames("tempabc123")).thenReturn(new String[]{"col1"});

            List<String> selectors = new ArrayList<>();
            selectors.add("Table__Col");

            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
                 MockedStatic<QSAliasToPhysicalConverter> converterMock = mockStatic(QSAliasToPhysicalConverter.class);
                 MockedConstruction<RInterpreter> interpMock = mockConstruction(RInterpreter.class,
                         (mock, context) -> {
                             when(mock.composeQuery()).thenReturn("testFrame[, c(\"col1\")]");
                         })) {

                utilMock.when(() -> Utility.getRandomString(6)).thenReturn("abc123");
                converterMock.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
                        .thenAnswer(inv -> inv.getArgument(0));

                List<SemossDataType> dataTypes = new ArrayList<>();
                dataTypes.add(SemossDataType.STRING);

                new RScaledUniqueFrameIterator(
                        frame, builder, "Column",
                        new Double[]{null}, new Double[]{null},
                        dataTypes, selectors);

                verify(builder).evalR("tempabc123 <- {testFrame[, c(\"col1\")]}");
            }
        }

        @Test
        void constructor_getsColumnHeaders() {
            when(metaData.getUniqueNameFromAlias("Column")).thenReturn("Table__Column");
            when(frame.getColumn("Table__Column")).thenReturn(new Object[]{"val1"});
            when(builder.getColumnNames("tempabc123")).thenReturn(new String[]{"col1", "col2"});

            List<String> selectors = new ArrayList<>();
            selectors.add("Table__Col");

            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
                 MockedStatic<QSAliasToPhysicalConverter> converterMock = mockStatic(QSAliasToPhysicalConverter.class);
                 MockedConstruction<RInterpreter> interpMock = mockConstruction(RInterpreter.class,
                         (mock, context) -> {
                             when(mock.composeQuery()).thenReturn("testFrame[, c(\"col1\")]");
                         })) {

                utilMock.when(() -> Utility.getRandomString(6)).thenReturn("abc123");
                converterMock.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
                        .thenAnswer(inv -> inv.getArgument(0));

                List<SemossDataType> dataTypes = new ArrayList<>();
                dataTypes.add(SemossDataType.STRING);

                new RScaledUniqueFrameIterator(
                        frame, builder, "Column",
                        new Double[]{null}, new Double[]{null},
                        dataTypes, selectors);

                verify(builder).getColumnNames("tempabc123");
            }
        }

        @Test
        void constructor_selectorWithDoubleDash_setsTableAndColumn() {
            when(metaData.getUniqueNameFromAlias("Column")).thenReturn("Table__Column");
            when(frame.getColumn("Table__Column")).thenReturn(new Object[]{"val1"});
            when(builder.getColumnNames("tempabc123")).thenReturn(new String[]{"col1"});

            List<String> selectors = new ArrayList<>();
            selectors.add("MyTable__MyCol");

            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
                 MockedStatic<QSAliasToPhysicalConverter> converterMock = mockStatic(QSAliasToPhysicalConverter.class);
                 MockedConstruction<RInterpreter> interpMock = mockConstruction(RInterpreter.class,
                         (mock, context) -> {
                             when(mock.composeQuery()).thenReturn("testFrame[, c(\"col1\")]");
                         })) {

                utilMock.when(() -> Utility.getRandomString(6)).thenReturn("abc123");
                converterMock.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
                        .thenAnswer(inv -> inv.getArgument(0));

                List<SemossDataType> dataTypes = new ArrayList<>();
                dataTypes.add(SemossDataType.STRING);

                RScaledUniqueFrameIterator iterator = new RScaledUniqueFrameIterator(
                        frame, builder, "Column",
                        new Double[]{null}, new Double[]{null},
                        dataTypes, selectors);

                assertNotNull(iterator);

                assertEquals(1, interpMock.constructed().size());
                RInterpreter constructedInterp = interpMock.constructed().get(0);
                verify(constructedInterp).setQueryStruct(any(SelectQueryStruct.class));
            }
        }

        @Test
        void constructor_selectorWithoutDoubleDash_setsTableAndPrimKey() {
            when(metaData.getUniqueNameFromAlias("Column")).thenReturn("Table__Column");
            when(frame.getColumn("Table__Column")).thenReturn(new Object[]{"val1"});
            when(builder.getColumnNames("tempabc123")).thenReturn(new String[]{"col1"});

            List<String> selectors = new ArrayList<>();
            selectors.add("SimpleTable");

            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
                 MockedStatic<QSAliasToPhysicalConverter> converterMock = mockStatic(QSAliasToPhysicalConverter.class);
                 MockedConstruction<RInterpreter> interpMock = mockConstruction(RInterpreter.class,
                         (mock, context) -> {
                             when(mock.composeQuery()).thenReturn("testFrame[, c(\"col1\")]");
                         })) {

                utilMock.when(() -> Utility.getRandomString(6)).thenReturn("abc123");
                converterMock.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
                        .thenAnswer(inv -> inv.getArgument(0));

                List<SemossDataType> dataTypes = new ArrayList<>();
                dataTypes.add(SemossDataType.STRING);

                RScaledUniqueFrameIterator iterator = new RScaledUniqueFrameIterator(
                        frame, builder, "Column",
                        new Double[]{null}, new Double[]{null},
                        dataTypes, selectors);

                assertNotNull(iterator);
                assertEquals(1, interpMock.constructed().size());
            }
        }

        @Test
        void constructor_withNormalization_createsArithmeticSelectors() {
            when(metaData.getUniqueNameFromAlias("Column")).thenReturn("Table__Column");
            when(frame.getColumn("Table__Column")).thenReturn(new Object[]{"val1"});
            when(builder.getColumnNames("tempabc123")).thenReturn(new String[]{"col1"});

            List<String> selectors = new ArrayList<>();
            selectors.add("Table__Col");

            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
                 MockedStatic<QSAliasToPhysicalConverter> converterMock = mockStatic(QSAliasToPhysicalConverter.class);
                 MockedConstruction<RInterpreter> interpMock = mockConstruction(RInterpreter.class,
                         (mock, context) -> {
                             when(mock.composeQuery()).thenReturn("testFrame[, c(\"col1\")]");
                         })) {

                utilMock.when(() -> Utility.getRandomString(6)).thenReturn("abc123");
                converterMock.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
                        .thenAnswer(inv -> inv.getArgument(0));

                List<SemossDataType> dataTypes = new ArrayList<>();
                dataTypes.add(SemossDataType.DOUBLE);

                RScaledUniqueFrameIterator iterator = new RScaledUniqueFrameIterator(
                        frame, builder, "Column",
                        new Double[]{10.0}, new Double[]{2.0},
                        dataTypes, selectors);

                assertNotNull(iterator);
                verify(builder).evalR(anyString());
            }
        }

        @Test
        void constructor_withoutNormalization_addsColumnSelector() {
            when(metaData.getUniqueNameFromAlias("Column")).thenReturn("Table__Column");
            when(frame.getColumn("Table__Column")).thenReturn(new Object[]{"val1"});
            when(builder.getColumnNames("tempabc123")).thenReturn(new String[]{"col1"});

            List<String> selectors = new ArrayList<>();
            selectors.add("Table__Col");

            try (MockedStatic<Utility> utilMock = mockStatic(Utility.class);
                 MockedStatic<QSAliasToPhysicalConverter> converterMock = mockStatic(QSAliasToPhysicalConverter.class);
                 MockedConstruction<RInterpreter> interpMock = mockConstruction(RInterpreter.class,
                         (mock, context) -> {
                             when(mock.composeQuery()).thenReturn("testFrame[, c(\"col1\")]");
                         })) {

                utilMock.when(() -> Utility.getRandomString(6)).thenReturn("abc123");
                converterMock.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
                        .thenAnswer(inv -> inv.getArgument(0));

                List<SemossDataType> dataTypes = new ArrayList<>();
                dataTypes.add(SemossDataType.STRING);

                RScaledUniqueFrameIterator iterator = new RScaledUniqueFrameIterator(
                        frame, builder, "Column",
                        new Double[]{null}, new Double[]{null},
                        dataTypes, selectors);

                assertNotNull(iterator);
                verify(builder).evalR(anyString());
            }
        }
    }

    // =========================================================================
    // HasNext Tests
    // =========================================================================

    @Nested
    class HasNextTests {

        @Test
        void hasNext_returnsTrue_whenValuesRemain() {
            RScaledUniqueFrameIterator iterator = createDefaultIterator();
            assertTrue(iterator.hasNext());
        }

        @Test
        void hasNext_returnsFalse_whenExhausted() {
            List<String> selectors = new ArrayList<>();
            selectors.add("Table__Col");

            List<SemossDataType> dataTypes = new ArrayList<>();
            dataTypes.add(SemossDataType.STRING);

            RScaledUniqueFrameIterator iterator = createIterator(
                    "Column",
                    "Table__Column",
                    new Object[]{},
                    new Double[]{null},
                    new Double[]{null},
                    dataTypes,
                    selectors,
                    new String[]{"col1"});

            assertFalse(iterator.hasNext());
        }
    }

    // =========================================================================
    // Next Tests
    // =========================================================================

    @Nested
    class NextTests {

        @Test
        void next_withStringValue_usesQuotes() {
            List<String> selectors = new ArrayList<>();
            selectors.add("Table__Col");

            List<SemossDataType> dataTypes = new ArrayList<>();
            dataTypes.add(SemossDataType.STRING);

            RScaledUniqueFrameIterator iterator = createIterator(
                    "Column",
                    "Table__Column",
                    new Object[]{"stringVal"},
                    new Double[]{null},
                    new Double[]{null},
                    dataTypes,
                    selectors,
                    new String[]{"col1"});

            List<Object[]> expected = new ArrayList<>();
            expected.add(new Object[]{"data1"});
            when(builder.getBulkDataRow(anyString(), any())).thenReturn(expected);

            iterator.next();

            verify(builder).getBulkDataRow(
                    eq("tempabc123[Column == \"stringVal\", ]"),
                    any(String[].class));
        }

        @Test
        void next_withNumberValue_noQuotes() {
            List<String> selectors = new ArrayList<>();
            selectors.add("Table__Col");

            List<SemossDataType> dataTypes = new ArrayList<>();
            dataTypes.add(SemossDataType.STRING);

            RScaledUniqueFrameIterator iterator = createIterator(
                    "Column",
                    "Table__Column",
                    new Object[]{42},
                    new Double[]{null},
                    new Double[]{null},
                    dataTypes,
                    selectors,
                    new String[]{"col1"});

            List<Object[]> expected = new ArrayList<>();
            expected.add(new Object[]{"data1"});
            when(builder.getBulkDataRow(anyString(), any())).thenReturn(expected);

            iterator.next();

            verify(builder).getBulkDataRow(
                    eq("tempabc123[Column == 42, ]"),
                    any(String[].class));
        }

        @Test
        void next_returnsDataFromBuilder() {
            RScaledUniqueFrameIterator iterator = createDefaultIterator();

            List<Object[]> expected = new ArrayList<>();
            expected.add(new Object[]{"row1col1"});
            expected.add(new Object[]{"row2col1"});
            when(builder.getBulkDataRow(anyString(), any())).thenReturn(expected);

            List<Object[]> result = iterator.next();

            assertSame(expected, result);
            verify(builder).getBulkDataRow(anyString(), any(String[].class));
        }

        @Test
        void next_throwsNoSuchElementException_whenExhausted() {
            List<String> selectors = new ArrayList<>();
            selectors.add("Table__Col");

            List<SemossDataType> dataTypes = new ArrayList<>();
            dataTypes.add(SemossDataType.STRING);

            RScaledUniqueFrameIterator iterator = createIterator(
                    "Column",
                    "Table__Column",
                    new Object[]{},
                    new Double[]{null},
                    new Double[]{null},
                    dataTypes,
                    selectors,
                    new String[]{"col1"});

            NoSuchElementException ex = assertThrows(NoSuchElementException.class, () -> iterator.next());
            assertEquals("No more elements", ex.getMessage());
        }
    }

    // =========================================================================
    // Iteration Tests
    // =========================================================================

    @Nested
    class IterationTests {

        @Test
        void iterateAllValues_consumesAllElements() {
            List<String> selectors = new ArrayList<>();
            selectors.add("Table__Col");

            List<SemossDataType> dataTypes = new ArrayList<>();
            dataTypes.add(SemossDataType.STRING);

            RScaledUniqueFrameIterator iterator = createIterator(
                    "Column",
                    "Table__Column",
                    new Object[]{"a", "b", "c"},
                    new Double[]{null},
                    new Double[]{null},
                    dataTypes,
                    selectors,
                    new String[]{"col1"});

            List<Object[]> mockData = new ArrayList<>();
            mockData.add(new Object[]{"data"});
            when(builder.getBulkDataRow(anyString(), any())).thenReturn(mockData);

            int count = 0;
            while (iterator.hasNext()) {
                List<Object[]> result = iterator.next();
                assertNotNull(result);
                count++;
            }

            assertEquals(3, count);
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, () -> iterator.next());
        }
    }
}
