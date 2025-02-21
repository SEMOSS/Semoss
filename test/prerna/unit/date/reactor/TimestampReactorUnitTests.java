package prerna.unit.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import prerna.date.SemossDate;
import prerna.date.reactor.TimestampReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class TimestampReactorUnitTests {

    @InjectMocks
    private TimestampReactor reactor;

    @Mock
    private SemossDate mockDate;

    private Map<String, String> keyValues;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        keyValues = new HashMap<>();
        reactor.keyValue = keyValues;
    }

    @Test
    void testDefaultTimestamp() {
        Calendar calendar = Calendar.getInstance();
        when(mockDate.getDate()).thenReturn(calendar.getTime());

        NounMetadata nm = reactor.execute();

        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        SemossDate date = (SemossDate) nm.getValue();
        assertEquals("yyyy-MM-dd HH:mm:ss", date.getPattern());
    }

    @Test
    void testTimestampWithDateOnly() {
        keyValues.put("date", "2022-03-19");

        NounMetadata nm = reactor.execute();

        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        SemossDate date = (SemossDate) nm.getValue();
        assertEquals("2022-03-19", date.getFormattedDate());
    }


    @Test
    void testTimestampWithInvalidDate() {
        keyValues.put("date", "invalid-date");

        NounMetadata nm = reactor.execute();

        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        SemossDate date = (SemossDate) nm.getValue();
        assertEquals("invalid-date", date.getFormattedDate());
    }

    @Test
    void testTimestampWithInvalidFormat() {
        keyValues.put("date", "2022-03-19");
        keyValues.put("format", "invalid-format");

        NounMetadata nm = reactor.execute();

        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        SemossDate date = (SemossDate) nm.getValue();
        assertEquals("2022-03-19", date.getFormattedDate());
    }
}
