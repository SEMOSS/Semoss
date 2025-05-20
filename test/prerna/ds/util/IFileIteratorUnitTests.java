package prerna.ds.util;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import prerna.ds.util.IFileIterator;

public class IFileIteratorUnitTests {

    @Mock
    private IFileIterator fileIterator;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGetNumRecordsOverSizeTrue() {
        long limitSize = 1000L;
        when(fileIterator.getNumRecordsOverSize(limitSize)).thenReturn(true);

        boolean result = fileIterator.getNumRecordsOverSize(limitSize);

        assertTrue(result, "Expected getNumRecordsOverSize to return true");
        verify(fileIterator, times(1)).getNumRecordsOverSize(limitSize);
    }
}