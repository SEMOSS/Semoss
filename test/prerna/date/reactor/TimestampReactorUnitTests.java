package prerna.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.junit.jupiter.api.Test;

import prerna.date.SemossDate;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class TimestampReactorUnitTests {
    TimestampReactor reactor; 

    @Test
    void test() {
        reactor = new TimestampReactor();
        reactor.keyValue.put("date", "2025-01-01");
        reactor.keyValue.put("format", "yyyy-MM-dd");

        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        assertInstanceOf(SemossDate.class, nm.getValue());

        reactor.keyValue.remove("date");
        nm = reactor.execute();
        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        assertInstanceOf(SemossDate.class, nm.getValue());
    }
}
