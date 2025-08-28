package prerna.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.date.SemossDay;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DayReactorUnitTests {
    DayReactor reactor;
    Map<String, String> keyValues;

    @Test
    void test() {
        reactor = new DayReactor();
        reactor.keyValue.put("days", "1");

        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_DAY, nm.getNounType());
        assertInstanceOf(SemossDay.class, nm.getValue());
        assertEquals(new SemossDay("1").getNumDays(), ((SemossDay) nm.getValue()).getNumDays());
    }
}
