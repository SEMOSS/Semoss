package prerna.unit.ds.util.flatfile;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.CALLS_REAL_METHODS;


import prerna.algorithm.api.SemossDataType;
import prerna.ds.util.flatfile.AbstractFileIterator;

public class AbstractFileIteratorUnitTests {

	private AbstractFileIterator fileIterator;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        fileIterator = Mockito.mock(AbstractFileIterator.class, CALLS_REAL_METHODS);
        setField(fileIterator, "fileLocation", "test.csv");
        setField(fileIterator, "headers", new String[] { "col1", "col2", "col3" });
        setField(fileIterator, "types", new SemossDataType[] { SemossDataType.INT, SemossDataType.DOUBLE, SemossDataType.BOOLEAN });
        setField(fileIterator, "additionalTypes", new String[] { null, null, null });
        setField(fileIterator, "nextRow", new Object[] { "1", "2.0", "true" });
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getSuperclass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getSuperclass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    @Test
    void testHasNextWithOffset() throws Exception {
        setField(fileIterator, "offset", 3);
        Mockito.doAnswer(invocation -> {
        	Long curOffset = (Long) getField(fileIterator, "curOffset");
            if (curOffset < 5) {
                setField(fileIterator, "nextRow", new Object[] { "1", "2.0", "true" });
                setField(fileIterator, "curOffset", curOffset + 1);
            } else {
                setField(fileIterator, "nextRow", null);
            }
            return null;
        }).when(fileIterator).getNextRow();
        assertTrue(fileIterator.hasNext());
    }

    @Test
    void testHasNextWithLimit() throws IOException, Exception {
        setField(fileIterator, "limit", 2);
        Mockito.doAnswer(invocation -> {
        	Long curOffset = (Long) getField(fileIterator, "curOffset");
            if (curOffset < 5) {
                setField(fileIterator, "nextRow", new Object[] { "1", "2.0", "true" });
                setField(fileIterator, "curOffset", curOffset + 1);
            } else {
                setField(fileIterator, "nextRow", null);
            }
            return null;
        }).when(fileIterator).getNextRow();
        assertTrue(fileIterator.hasNext());
        fileIterator.getNextRow();
        assertTrue(fileIterator.hasNext());
        fileIterator.getNextRow();
    }

    @Test
    void testNext() throws Exception {
        setField(fileIterator, "offset", 1);
        setField(fileIterator, "limit", 2);
        Mockito.doAnswer(invocation -> {
            Long curOffset = (Long) getField(fileIterator, "curOffset");
            if (curOffset < 5) {
                setField(fileIterator, "nextRow", new Object[] { "1", "2.0", "true" });
                setField(fileIterator, "curOffset", curOffset + 1);
            } else {
                setField(fileIterator, "nextRow", null);
            }
            return null;
        }).when(fileIterator).getNextRow();
        assertTrue(fileIterator.hasNext());
        assertNotNull(fileIterator.next());
        assertTrue(fileIterator.hasNext());
        assertNotNull(fileIterator.next());
        assertFalse(fileIterator.hasNext());
    }

    @Test
    void testCleanRow() throws Exception {
        Object[] row = { "1", "2.0", "true" };
        Method cleanRowMethod = AbstractFileIterator.class.getDeclaredMethod("cleanRow", Object[].class, SemossDataType[].class, String[].class);
        cleanRowMethod.setAccessible(true);
        Object[] cleanRow = (Object[]) cleanRowMethod.invoke(fileIterator, row, getField(fileIterator, "types"), getField(fileIterator, "additionalTypes"));
        assertEquals(1, cleanRow[0]);
        assertEquals(2.0, cleanRow[1]);
        assertEquals(true, cleanRow[2]);
    }

    @Test
    void testDeleteFile() throws IOException, Exception {
        fileIterator.deleteFile();
        File file = new File((String) getField(fileIterator, "fileLocation"));
        assertFalse(file.exists());
    }
    @Test
    void testgetFileLocation() throws IOException, Exception {
        String file = fileIterator.getFileLocation();
        assertNotNull(file);
    }

    @Test
    void testGetTypes() throws Exception {
        assertArrayEquals(new SemossDataType[] { SemossDataType.INT, SemossDataType.DOUBLE, SemossDataType.BOOLEAN }, fileIterator.getTypes());
    }

    @Test
    void testGetHeaders() throws Exception {
        assertArrayEquals(new String[] { "col1", "col2", "col3" }, fileIterator.getHeaders());
    }

    @Test
    void testGetNumRows() throws Exception {
        setField(fileIterator, "numRecords", 9);
        assertEquals(3, fileIterator.getNumRows());
    }

    @Test
    void testGetNumRecords() throws Exception {
        setField(fileIterator, "numRecords", 9);
        assertEquals(9, fileIterator.getNumRecords());
    }
    @Test
    void testHasNextWithEmptyFile() throws Exception {
        setField(fileIterator, "nextRow", null);
        assertFalse(fileIterator.hasNext());
    }

    @Test
    void testHasNextWithOnlyHeaders() throws Exception {
        setField(fileIterator, "nextRow", null);
        setField(fileIterator, "curOffset", 0);
        setField(fileIterator, "offset", 0);
        setField(fileIterator, "limit", 0);

        Mockito.doAnswer(invocation -> {
            setField(fileIterator, "nextRow", null);
            return null;
        }).when(fileIterator).getNextRow();

        assertFalse(fileIterator.hasNext());
    }
    @Test
    void testGetNumRecordsOverSize() throws Exception {
        setField(fileIterator, "headers", new String[] { "col1", "col2", "col3" });
        setField(fileIterator, "curOffset", 0);
        setField(fileIterator, "nextRow", new Object[] { "1", "2.0", "true" });

        Mockito.doAnswer(invocation -> {
            Long curOffset = (Long) getField(fileIterator, "curOffset");
            if (curOffset < 5) {
                setField(fileIterator, "nextRow", new Object[] { "1", "2.0", "true" });
                setField(fileIterator, "curOffset", curOffset + 1);
            } else {
                setField(fileIterator, "nextRow", null);
            }
            return null;
        }).when(fileIterator).getNextRow();

        Mockito.doAnswer(invocation -> {
            Long curOffset = (Long) getField(fileIterator, "curOffset");
            return curOffset < 5;
        }).when(fileIterator).hasNext();

        long limit = 10;
        boolean result = fileIterator.getNumRecordsOverSize(limit);
        assertTrue(result);

        setField(fileIterator, "curOffset", 0);
        Mockito.doAnswer(invocation -> {
            Long curOffset = (Long) getField(fileIterator, "curOffset");
            if (curOffset < 2) {
                setField(fileIterator, "nextRow", new Object[] { "1", "2.0", "true" });
                setField(fileIterator, "curOffset", curOffset + 1);
            } else {
                setField(fileIterator, "nextRow", null);
            }
            return null;
        }).when(fileIterator).getNextRow();

        Mockito.doAnswer(invocation -> {
            Long curOffset = (Long) getField(fileIterator, "curOffset");
            return curOffset < 2;
        }).when(fileIterator).hasNext();

        limit = 10;
        result = fileIterator.getNumRecordsOverSize(limit);
        assertFalse(result);
        assertEquals(6L, getField(fileIterator, "numRecords")); // 2 rows * 3 headers = 6
    }
}