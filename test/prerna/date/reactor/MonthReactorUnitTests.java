package prerna.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.junit.jupiter.api.Test;

import prerna.date.SemossMonth;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MonthReactorUnitTests {
    MonthReactor reactor;

    @Test
    void test() {
        reactor = new MonthReactor();
        reactor.keyValue.put("months", "1");

        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_MONTH, nm.getNounType());
        assertInstanceOf(SemossMonth.class, nm.getValue());
        assertEquals(new SemossMonth("1").getNumMonths(), ((SemossMonth) nm.getValue()).getNumMonths());
    }
}
