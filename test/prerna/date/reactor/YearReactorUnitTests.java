package prerna.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.junit.jupiter.api.Test;

import prerna.date.SemossYear;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class YearReactorUnitTests {
    YearReactor reactor;

    @Test
    void test() {
        reactor = new YearReactor();
        reactor.keyValue.put("years", "1");

        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_YEAR, nm.getNounType());
        assertInstanceOf(SemossYear.class, nm.getValue());
        assertEquals(new SemossYear("1").getNumYears(), ((SemossYear) nm.getValue()).getNumYears());
    }
}
