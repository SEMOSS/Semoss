package prerna.unit.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.date.SemossMonth;
import prerna.date.reactor.MonthReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MonthReactorUnitTests {

    private MonthReactor reactor;
    private Map<String, String> keyValues;

    @BeforeEach
    void setup() {
        reactor = new MonthReactor();
        keyValues = new HashMap<>();
        reactor.keyValue = keyValues;
    }

    @Test
    void testMonthReactorWithValidMonths() {
        keyValues.put("months", "12");

        NounMetadata nm = reactor.execute();

        assertEquals(PixelDataType.CONST_MONTH, nm.getNounType());
        SemossMonth month = (SemossMonth) nm.getValue();
        assertEquals(12, month.getNumMonths());
    }

    @Test
    void testMonthReactorWithNoMonths() {
        NounMetadata nm = reactor.execute();

        assertEquals(PixelDataType.CONST_MONTH, nm.getNounType());
        SemossMonth month = (SemossMonth) nm.getValue();
        assertEquals(null, (Integer) month.getNumMonths());
    }

    @Test
    void testMonthReactorWithInvalidMonths() {
        keyValues.put("months", "invalid");

        NounMetadata nm = reactor.execute();

        assertEquals(PixelDataType.CONST_MONTH, nm.getNounType());
        SemossMonth month = (SemossMonth) nm.getValue();
        assertEquals("invalid", month.getNumMonths());
    }
}
