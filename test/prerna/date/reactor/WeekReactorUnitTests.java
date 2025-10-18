package prerna.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.junit.jupiter.api.Test;

import prerna.date.SemossWeek;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class WeekReactorUnitTests {
    WeekReactor reactor;

    @Test
    void test() {
        reactor = new WeekReactor();
        reactor.keyValue.put("weeks", "1");

        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_WEEK, nm.getNounType());
        assertInstanceOf(SemossWeek.class, nm.getValue());
        assertEquals(new SemossWeek("1").getNumWeeks(), ((SemossWeek) nm.getValue()).getNumWeeks());
    }
}
