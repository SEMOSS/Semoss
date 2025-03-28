package prerna.unit.ds.util.flatfile;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.query.querystruct.CsvQueryStruct;
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
    void testGetNextRow() throws Exception {
        assertArrayEquals(new String[]{"1", "2", "3"}, getNextRow(fileIterator));

        fileIterator.getNextRow();
        assertArrayEquals(new String[]{"4", "5", "6"}, getNextRow(fileIterator));

        fileIterator.getNextRow();
        assertArrayEquals(new String[]{"7", "8", "9"}, getNextRow(fileIterator));

        fileIterator.getNextRow();
        assertNull(getNextRow(fileIterator));
    }

    @Test
    void testSetSelectors() throws Exception {
        QueryColumnSelector selector = new QueryColumnSelector("col1");
        when(mockQueryStruct.getSelectors()).thenReturn(Arrays.asList(selector));
        java.lang.reflect.Method setSelectorsMethod = CsvFileIterator.class.getDeclaredMethod("setSelectors", List.class);
        setSelectorsMethod.setAccessible(true);
        setSelectorsMethod.invoke(fileIterator, mockQueryStruct.getSelectors());

        String[] headers = fileIterator.getHelper().getHeaders();
        assertArrayEquals(new String[]{"col1"}, headers);
    }

    @Test
    void testReset() throws Exception {
        java.lang.reflect.Method resetMethod = CsvFileIterator.class.getDeclaredMethod("reset");
        resetMethod.setAccessible(true);
        resetMethod.invoke(fileIterator);
    }

    @Test
    void testClose() throws IOException {
        fileIterator.close();
    }

    @Test
    void testSetUnknownTypes() throws Exception {
        java.lang.reflect.Method setUnknownTypesMethod = CsvFileIterator.class.getDeclaredMethod("setUnknownTypes");
        setUnknownTypesMethod.setAccessible(true);
        setUnknownTypesMethod.invoke(fileIterator);
    }

    @Test
    void testSetQs() {
        CsvQueryStruct newQueryStruct = mock(CsvQueryStruct.class);
        fileIterator.setQs(newQueryStruct);
        assertEquals(newQueryStruct, fileIterator.getQs());
    }
    private String[] getNextRow(CsvFileIterator iterator) throws Exception {
        java.lang.reflect.Field nextRowField = CsvFileIterator.class.getSuperclass().getDeclaredField("nextRow");
        nextRowField.setAccessible(true);
        return (String[]) nextRowField.get(iterator);
    }
}