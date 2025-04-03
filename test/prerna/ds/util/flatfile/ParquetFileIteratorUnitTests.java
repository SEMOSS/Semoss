package prerna.ds.util.flatfile;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import prerna.ds.util.flatfile.ParquetFileIterator;
import prerna.poi.main.helper.ParquetFileHelper;
import prerna.query.querystruct.ParquetQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;

class ParquetFileIteratorUnitTests {

    @Mock
    private ParquetFileHelper mockHelper;

    @Mock
    private ParquetQueryStruct mockQueryStruct;

    private ParquetFileIterator iterator;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        when(mockQueryStruct.getFilePath()).thenReturn("test.parquet");

        Map<String, String> columnTypes = new HashMap<>();
        columnTypes.put("name", "STRING");
        columnTypes.put("age", "INTEGER");
        when(mockQueryStruct.getColumnTypes()).thenReturn(columnTypes);

        List<IQuerySelector> selectors = new ArrayList<>();
        selectors.add(new QueryColumnSelector("name"));
        selectors.add(new QueryColumnSelector("age"));
        when(mockQueryStruct.getSelectors()).thenReturn(selectors);

        when(mockQueryStruct.getLimit()).thenReturn(10L);
        when(mockQueryStruct.getOffset()).thenReturn(0L);

        try (MockedConstruction<ParquetFileHelper> mocked = Mockito.mockConstruction(ParquetFileHelper.class, (mock, context) -> {
            when(mock.getHeaders()).thenReturn(new String[]{"name", "age"});
        })) {
            iterator = new ParquetFileIterator(mockQueryStruct);
        }
    }

    @Test
    void testConstructor_initialization() {
        assertEquals("test.parquet", iterator.getQs().getFilePath());
        assertNotNull(iterator.getQs().getColumnTypes());
        assertEquals(10, iterator.getQs().getLimit());
        assertEquals(0, iterator.getQs().getOffset());
    }


    @Test
    void testSetSelectors_withValidSelectors() {
        List<IQuerySelector> selectors = new ArrayList<>();
        selectors.add(new QueryColumnSelector("name"));
        selectors.add(new QueryColumnSelector("age"));

        when(mockHelper.getHeaders()).thenReturn(new String[]{"name", "age"});

        assertDoesNotThrow(() -> iterator.getQs().setSelectors(selectors));

        assertEquals(2, iterator.getQs().getSelectors().size());
    }

    @Test
    void testSetSelectors_withInvalidSelectorType() {
        QueryColumnSelector invalidSelector = mock(QueryColumnSelector.class);
        when(invalidSelector.getSelectorType()).thenReturn(IQuerySelector.SELECTOR_TYPE.ARITHMETIC);

        when(mockQueryStruct.getSelectors()).thenReturn(Arrays.asList(invalidSelector));

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> {
            new ParquetFileIterator(mockQueryStruct);
        });

        assertEquals("Cannot perform math on a csv import", thrown.getMessage());
    }

    @Test
    void testReset() throws Exception {
        assertDoesNotThrow(() -> iterator.reset());
    }

    @Test
    void testClose() throws IOException {
        assertDoesNotThrow(() -> iterator.close());
    }

    @Test
    void testGetNextRow() {
        assertDoesNotThrow(() -> iterator.getNextRow());
    }
    
    @Test
    void testModifyCleanedHeaders_withUserDefinedHeaders() {
        Map<String, String> userDefinedHeaders = new HashMap<>();
        userDefinedHeaders.put("new_name", "name");
        userDefinedHeaders.put("new_age", "age");

        when(mockQueryStruct.getNewHeaderNames()).thenReturn(userDefinedHeaders);

        try (MockedConstruction<ParquetFileHelper> mocked = Mockito.mockConstruction(ParquetFileHelper.class, (mock, context) -> {
            when(mock.getHeaders()).thenReturn(new String[]{"name", "age"});
        })) {
            iterator = new ParquetFileIterator(mockQueryStruct);

            ParquetFileHelper helperInstance = mocked.constructed().get(0);
            Mockito.verify(helperInstance).modifyCleanedHeaders(userDefinedHeaders);
        }
    }
    @Test
    void testSetSelectors_withEmptySelectors() {
        List<IQuerySelector> emptySelectors = new ArrayList<>();
        when(mockQueryStruct.getSelectors()).thenReturn(emptySelectors);

        try (MockedConstruction<ParquetFileHelper> mocked = Mockito.mockConstruction(ParquetFileHelper.class, (mock, context) -> {
            when(mock.getHeaders()).thenReturn(new String[]{"name", "age"});
        })) {
            iterator = new ParquetFileIterator(mockQueryStruct);

            List<IQuerySelector> selectors = iterator.getQs().getSelectors();
            assertEquals(0, selectors.size());
        }
    }
    
    @Test
    void testSetSelectors_withMismatchedHeadersAndSelectors() {
        List<IQuerySelector> selectors = new ArrayList<>();
        selectors.add(new QueryColumnSelector("name"));
        selectors.add(new QueryColumnSelector("age"));
        when(mockQueryStruct.getSelectors()).thenReturn(selectors);

        try (MockedConstruction<ParquetFileHelper> mocked = Mockito.mockConstruction(ParquetFileHelper.class, (mock, context) -> {
            when(mock.getHeaders()).thenReturn(new String[]{"name", "age", "extra"});
        })) {
            iterator = new ParquetFileIterator(mockQueryStruct);

            ParquetFileHelper helperInstance = mocked.constructed().get(0);
            Mockito.verify(helperInstance).parseColumns(new String[]{"name", "age"});
        }
    }
    @Test
    void testSetSelectors_withKnownTypes() {
        List<IQuerySelector> selectors = new ArrayList<>();
        selectors.add(new QueryColumnSelector("name"));
        selectors.add(new QueryColumnSelector("age"));
        when(mockQueryStruct.getSelectors()).thenReturn(selectors);

        Map<String, String> dataTypeMap = new HashMap<>();
        Map<String, String> additionalTypesMap = new HashMap<>();
        when(mockQueryStruct.getColumnTypes()).thenReturn(dataTypeMap);
        when(mockQueryStruct.getAdditionalTypes()).thenReturn(additionalTypesMap);
        
        try (MockedConstruction<ParquetFileHelper> mocked = Mockito.mockConstruction(ParquetFileHelper.class, (mock, context) -> {
            when(mock.getHeaders()).thenReturn(new String[]{"name", "age"});
        })) {
            iterator = new ParquetFileIterator(mockQueryStruct);

            Mockito.verify(mockQueryStruct).setColumnTypes(dataTypeMap);
            
        }
    }
}