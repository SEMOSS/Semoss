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
package prerna.ds.shared;

import com.google.gson.TypeAdapter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.crypto.Cipher;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.SemossUnitTest;
import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.SemossDataType;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter.QUERY_FILTER_TYPE;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;

public class AbstractTableDataFrameUnitTests extends SemossUnitTest {
    Cipher cipher;
    GenRowFilters grf;
    IQueryFilter iFilter;
    SelectQueryStruct sqs;
    IHeadersDataRow dataRow;
    CachePropFileFrameObject cf;
    DataFrameTypeEnum frameType;
    IRawSelectWrapper rawWrapper;
    AbstractTableDataFrame reactor;
    OwlTemporalEngineMeta metaData;

    List<String> selectors;

    @BeforeEach
    void setup() {
        cipher = mock(Cipher.class);
        grf = mock(GenRowFilters.class);
        iFilter = mock(IQueryFilter.class);
        sqs = mock(SelectQueryStruct.class);
        dataRow = mock(IHeadersDataRow.class);
        frameType = mock(DataFrameTypeEnum.class);
        cf = mock(CachePropFileFrameObject.class);
        rawWrapper = mock(IRawSelectWrapper.class);
        metaData = mock(OwlTemporalEngineMeta.class);

        selectors = new ArrayList<>();
        selectors.add("table__col1");

        reactor = new AbstractTableDataFrame(){
            @Override
            public void addRow(Object[] cleanCells, String[] headers) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'addRow'");
            }

            @Override
            public void removeColumn(String columnHeader) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'removeColumn'");
            }

            @Override
            public boolean isEmpty() {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'isEmpty'");
            }

            @Override
            public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'save'");
            }

            @Override
            public void open(CachePropFileFrameObject cf, Cipher cipher) throws IOException {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'open'");
            }

            @Override
            public long size(String tableName) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'size'");
            }

            @Override
            public IRawSelectWrapper query(String query) throws Exception {
                if (query == null) throw new Exception();
                return rawWrapper;
            }

            @Override
            public IRawSelectWrapper query(SelectQueryStruct qs) throws Exception {
                if (qs == null) throw new Exception();
                return rawWrapper;
            }

            @Override
            public IQueryInterpreter getQueryInterpreter() {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'getQueryInterpreter'");
            }

            @Override
            public DataFrameTypeEnum getFrameType() {
                return frameType;
            }

            @Override
            public void processDataMakerComponent(DataMakerComponent component) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'processDataMakerComponent'");
            }

            @Override
            public String getDataMakerName() {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'getDataMakerName'");
            }
        };

        when(metaData.getFrameSelectors()).thenReturn(selectors);

        reactor.setFrameFilters(grf);
        reactor.setMetaData(metaData);
    }

    @AfterEach
    void cleanDirectory() throws IOException {
        if (Files.exists(tempDir)) {
            FileUtils.cleanDirectory(tempDir.toFile());
        }
    }

    @Test
    void getFrameHeadersObject() {
        String[] types = new String[]{""};
        Set<String> filteredCols = new HashSet<>();
        filteredCols.add("aliasVal");
        List<Map<String, Object>> headersMap = new ArrayList<>();
        headersMap.add(new HashMap<String, Object>(){{
            put("alias", "aliasVal");
            put("header", "headerVal");
        }});
        headersMap.add(new HashMap<String, Object>(){{
            put("alias", "aliasVal2");
            put("header", "headerVal2");
        }});

        Map<String, Object> headersObj = new HashMap<>();
        headersObj.put("headers", headersMap);
        Map<String, Object> expected = new HashMap<>();
        expected.put("name", "frameName");
        expected.put("type", "types");
        expected.put("headerInfo", headersObj);

        when(metaData.getTableHeaderObjects(types)).thenReturn(headersObj);
        when(grf.getAllFilteredColumns()).thenReturn(filteredCols);

        when(frameType.getTypeAsString()).thenReturn("types");
        
        reactor.setName("frameName");

        assertEquals(expected, reactor.getFrameHeadersObject(types));

        assertEquals(grf, reactor.getFrameFilters());
        assertEquals(metaData, reactor.getMetaData());
    }

    @Test
    void isUniqueColumn() throws Exception {
        when(metaData.getUniqueNameFromAlias("table__col")).thenReturn(null);
        when(metaData.getUniqueNameFromAlias("col1")).thenReturn("col1");

        when(rawWrapper.next())
            .thenThrow(NoSuchElementException.class)
            .thenReturn(dataRow)
            .thenThrow(NoSuchElementException.class)
            .thenReturn(dataRow);
        when(dataRow.getValues()).thenReturn(new Object[]{1});

        doThrow(IOException.class)
            .doNothing()    
            .doThrow(IOException.class)
            .doNothing().when(rawWrapper).close();

        SemossPixelException noSuchElementException = assertThrows(SemossPixelException.class, () -> reactor.isUniqueColumn("table__col"));
        noSuchElementException = assertThrows(SemossPixelException.class, () -> reactor.isUniqueColumn("table__col"));
        assertTrue(reactor.isUniqueColumn("col1"));
        assertTrue(reactor.isUniqueColumn("col1"));
    }

    @Test
    void getColumnHeaders() {
        List<String> headers = new ArrayList<>();
        headers.add("header1");
        String[] expected = headers.toArray(new String[headers.size()]);

        when(metaData.getOrderedAliasOrUniqueNames()).thenReturn(headers);

        assertArrayEquals(expected, reactor.getColumnHeaders());
    }

    @Test
    void getQsHeaders() {
        String[] expected = selectors.toArray(new String[selectors.size()]);

        reactor.qsNames = null;
        assertArrayEquals(expected, reactor.getQsHeaders());
    }

    @Test
    void isNumeric() {
        boolean[] expected = new boolean[]{true};

        when(metaData.getHeaderTypeAsEnum("table__col1")).thenReturn(SemossDataType.INT);

        assertArrayEquals(expected, reactor.isNumeric());
    }

    @Test
    void getColumn() throws Exception {
        Object[] expected = new Object[]{""};

        when(rawWrapper.hasNext()).thenReturn(true).thenReturn(false).thenReturn(true);
        when(rawWrapper.next()).thenReturn(dataRow).thenThrow(NoSuchElementException.class);
        when(dataRow.getValues()).thenReturn(expected);
        
        doNothing().doThrow(IOException.class).when(rawWrapper).close();

        assertArrayEquals(expected, reactor.getColumn("header"));
        SemossPixelException e = assertThrows(SemossPixelException.class, () -> reactor.getColumn("header"));
    }

    @Test
    void getColumnAsNumeric() throws Exception {
        Double[] expected = new Double[]{1.0};

        when(rawWrapper.hasNext()).thenReturn(true).thenReturn(false).thenReturn(true);
        when(rawWrapper.next()).thenReturn(dataRow).thenThrow(NoSuchElementException.class);
        when(dataRow.getValues()).thenReturn(expected);
        
        doNothing().doThrow(IOException.class).when(rawWrapper).close();

        assertArrayEquals(expected, reactor.getColumnAsNumeric("header"));
        SemossPixelException e = assertThrows(SemossPixelException.class, () -> reactor.getColumnAsNumeric("header"));
    }

    @Test
    void getMax() throws Exception {
        Double[] expected = new Double[]{1.0};

        when(metaData.getHeaderTypeAsEnum("name")).thenReturn(SemossDataType.INT);
        when(metaData.getHeaderTypeAsEnum("newName")).thenReturn(SemossDataType.INT);
        when(metaData.getUniqueNameFromAlias("name")).thenReturn(null);
        when(metaData.getUniqueNameFromAlias("newName")).thenReturn(null);

        when(rawWrapper.hasNext()).thenReturn(true).thenReturn(true);
        when(rawWrapper.next()).thenReturn(dataRow).thenThrow(NoSuchElementException.class);
        when(dataRow.getValues()).thenReturn(new Double[]{1.0});
        
        doNothing().doThrow(IOException.class).when(rawWrapper).close();

        reactor.qsNames = new String[]{"name"};
        assertArrayEquals(expected, reactor.getMax());
        reactor.qsNames = new String[]{"newName"};
        assertArrayEquals(new Double[]{null}, reactor.getMax());
        
        reactor.uniqueColumnMaxCache.put("name", 2.0);
        reactor.qsNames = new String[]{"name"};
        assertArrayEquals(new Double[]{2.0}, reactor.getMax());
    }

    @Test
    void getMin() throws Exception {
        Double[] expected = new Double[]{1.0};

        when(metaData.getUniqueNameFromAlias("name")).thenReturn(null);
        when(metaData.getUniqueNameFromAlias("newName")).thenReturn(null);
        when(metaData.getHeaderTypeAsEnum("name")).thenReturn(SemossDataType.INT);
        when(metaData.getHeaderTypeAsEnum("newName")).thenReturn(SemossDataType.INT);

        when(rawWrapper.hasNext()).thenReturn(true).thenReturn(true);
        when(rawWrapper.next()).thenReturn(dataRow).thenThrow(NoSuchElementException.class);
        when(dataRow.getValues()).thenReturn(new Double[]{1.0});
        
        doNothing().doThrow(IOException.class).when(rawWrapper).close();

        reactor.qsNames = new String[]{"name"};
        assertArrayEquals(expected, reactor.getMin());
        reactor.qsNames = new String[]{"newName"};
        assertArrayEquals(new Double[]{null}, reactor.getMin());

        reactor.uniqueColumnMinCache.put("name", 2.0);
        reactor.qsNames = new String[]{"name"};
        assertArrayEquals(new Double[]{2.0}, reactor.getMin());
    }

    @Test
    void settersAndGetters() {
        reactor.setUserId("id");
        assertEquals("id", reactor.getUserId());

        reactor.updateDataId();
        assertEquals(1, reactor.getDataId());
        
        reactor.resetDataId();
        assertEquals(0, reactor.getDataId());

        reactor.setName("name");
        assertEquals("name", reactor.getName());
        
        reactor.setOriginalName("name");
        assertEquals("name", reactor.getOriginalName());

        assertFalse(reactor.isClosed());

        assertEquals("", reactor.getFilterString());
        
        reactor.setLogger(null);
        assertNull(reactor.logger);
    }

    @Test
    void addSetAndUnfilter() {
        Set<String> allColUsed = new HashSet<>();

        when(grf.isEmpty()).thenReturn(true).thenReturn(false);
        when(grf.getAllFilteredColumns()).thenReturn(allColUsed);
        when(grf.removeColumnFilter("")).thenReturn(true).thenReturn(false);

        reactor.addFilter(grf);
        reactor.addFilter(iFilter);
        reactor.setFilter(grf);
        reactor.unfilter();
        reactor.unfilter();
        reactor.unfilter("");
        reactor.unfilter("");

        verify(grf, times(2)).merge(grf);
        verify(grf, times(1)).merge(iFilter);

        verify(grf).getAllFilteredColumns();
        verify(grf).removeColumnFilters(allColUsed);
        verify(grf, times(2)).removeColumnFilter("");

        verify(grf, times(2)).isEmpty();
        verify(grf).removeAllFilters();
    }

    @Test
    void saveMeta() throws Exception {
        Path metaFileName = tempDir.resolve("METADATA__name.owl");
        Files.createFile(metaFileName);
        Path frameStateFileName = tempDir.resolve("FRAME_STATE__name.json");
        Files.createFile(frameStateFileName);
        
        List<IQueryFilter> filters = new ArrayList<>();
        filters.add(iFilter);
        
        when(grf.getFilters()).thenReturn(filters);

        try(MockedStatic<IQueryFilter> iFilterStatic = Mockito.mockStatic(IQueryFilter.class)) {
            when(iFilter.getQueryFilterType()).thenReturn(QUERY_FILTER_TYPE.SIMPLE);
            iFilterStatic.when(() -> IQueryFilter.getAdapterForFilter(QUERY_FILTER_TYPE.SIMPLE)).thenReturn(mock(TypeAdapter.class));

            reactor.saveMeta(cf, tempDir.toAbsolutePath().toString(), "name", cipher);
            reactor.saveMeta(cf, tempDir.toAbsolutePath().toString(), "name", null);

            verify(grf, times(4)).getFilters();
            verify(iFilter, times(2)).getQueryFilterType();
            iFilterStatic.verify(() -> IQueryFilter.getAdapterForFilter(QUERY_FILTER_TYPE.SIMPLE), times(2));
        }
    }

    @Test
    void openCacheMeta() throws Exception {
        Path metaFileName = tempDir.resolve("METADATA__name.owl");
        Files.createFile(metaFileName);
        Path frameStateFileName = tempDir.resolve("FRAME_STATE__name.json");
        Files.createFile(frameStateFileName);

        when(cf.getFrameName()).thenReturn("frameName");
        when(cf.getFrameMetaCacheLocation()).thenReturn(metaFileName.toAbsolutePath().toString());
        when(cf.getFrameStateCacheLocation()).thenReturn(frameStateFileName.toAbsolutePath().toString());

        reactor.openCacheMeta(cf, cipher);
        reactor.openCacheMeta(cf, null);

        verify(cf, times(2)).getFrameName();
        verify(cf, times(2)).getFrameMetaCacheLocation();
        verify(cf, times(2)).getFrameStateCacheLocation();
    }

    @Test
    void getData() throws Exception {
        Object[] obj = new Object[]{null};
        List<Object[]> expected = new ArrayList<>();
        expected.add(obj);

        when(metaData.getFlatTableQs(false)).thenReturn(sqs);
        when(rawWrapper.hasNext()).thenReturn(true).thenReturn(false).thenReturn(true);
        when(rawWrapper.next()).thenReturn(dataRow).thenThrow(NoSuchElementException.class);
        when(dataRow.getValues()).thenReturn(obj);
        doNothing().doThrow(IOException.class).when(rawWrapper).close();

        assertEquals(expected, reactor.getData());
        SemossPixelException e = assertThrows(SemossPixelException.class, () -> reactor.getData());
    }

    @Test
    void iterator() {
        when(metaData.getFlatTableQs(false)).thenReturn(sqs);

        assertEquals(rawWrapper, reactor.iterator());
    }

    @Test
    void scaledUniqueIterator() {
        List<String> list = new ArrayList<>();
        list.add("string");

        when(metaData.getUniqueNameFromAlias("string")).thenReturn(null);
        when(metaData.getHeaderTypeAsEnum("string")).thenReturn(SemossDataType.INT);

        assertNotNull(reactor.scaledUniqueIterator("colName", list));
    }

    @Test
    void getUniqueInstanceCount() throws Exception {
        Object[] obj = new Object[]{1};

        when(rawWrapper.hasNext()).thenReturn(true).thenReturn(true);
        when(rawWrapper.next()).thenReturn(dataRow).thenThrow(NoSuchElementException.class);
        when(dataRow.getValues()).thenReturn(obj);
        doNothing().doThrow(IOException.class).when(rawWrapper).close();

        assertEquals(1, reactor.getUniqueInstanceCount("table__col"));
        assertEquals(0, reactor.getUniqueInstanceCount("table"));
    }

    @Test
    void clearQueryCache() {
        reactor.clearQueryCache();
    }

    @Test
    void cacheQuery() {
        CachedIterator it = mock(CachedIterator.class);
        when(it.hasNext()).thenReturn(true).thenReturn(false);
        when(it.getQuery()).thenReturn("");
        
        reactor.cacheQuery(it);

        verify(it).hasNext();
        verify(it).getQuery();
    }

    @Test
    void getScriptReactors() {
        // assertNotNull(reactor.getScriptReactors());
        Map<String, String> expected = new HashMap<>();
        assertTrue(expected.equals(reactor.getScriptReactors()));
    }

    @Test
    void processPreTransformations() {
        DataMakerComponent dmc = mock(DataMakerComponent.class);
        ISEMOSSTransformation semossTransform = mock(ISEMOSSTransformation.class);
        List<ISEMOSSTransformation> list = new ArrayList<>();
        list.add(semossTransform);

        reactor.processPreTransformations(dmc, list);

        verify(semossTransform).setDataMakers(any(IDataMaker.class));
        verify(semossTransform).setDataMakerComponent(dmc);
        verify(semossTransform).runMethod();
    }

    @Test
    void processPostTransformations() {
        IDataMaker[] dataMaker = new IDataMaker[]{null};
        DataMakerComponent dmc = mock(DataMakerComponent.class);
        ISEMOSSTransformation semossTransform = mock(ISEMOSSTransformation.class);
        List<ISEMOSSTransformation> list = new ArrayList<>();
        list.add(semossTransform);

        reactor.processPostTransformations(dmc, list, dataMaker);

        verify(semossTransform).setDataMakers(any(IDataMaker[].class));
        verify(semossTransform).setDataMakerComponent(dmc);
        verify(semossTransform).runMethod();
    }

    @Test
    void getDataMakerOutput() {
        assertNotNull(reactor.getDataMakerOutput(new String[0]));
        assertNotNull(reactor.getDataMakerOutput(new String[]{"selector"}));
    }

    @Test
    void throwsErrors() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> reactor.querySQL(""));
        assertEquals("Method not implemented for frame = ", e.getMessage());
        e = assertThrows(IllegalArgumentException.class, () -> reactor.queryCSV(""));
        assertEquals("Method not implemented for frame = ", e.getMessage());
        e = assertThrows(IllegalArgumentException.class, () -> reactor.queryJSON(""));
        assertEquals("Method not implemented for frame = ", e.getMessage());
        e = assertThrows(IllegalArgumentException.class, () -> reactor.createVarFrame());
    }

    @Test
    void close() {
        reactor.close();
    }

    @Test
    void finalizeTest() throws Throwable {
        reactor.finalize();
    }
}
