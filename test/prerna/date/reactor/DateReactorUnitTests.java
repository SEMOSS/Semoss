package prerna.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.junit.jupiter.api.Test;

import prerna.date.SemossDate;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DateReactorUnitTests {
    DateReactor reactor;

    @Test
    void test() {
        reactor = new DateReactor();
        reactor.keyValue.put("date", "2025-01-01");
        reactor.keyValue.put("format", "yyyy-MM-dd");

        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        assertInstanceOf(SemossDate.class, nm.getValue());

        assertEquals("Get todays date or return a date based on a specific date input and format", reactor.getReactorDescription());
        assertEquals("A specific date to return. This is a string and assumes a date of yyyy-MM-dd", reactor.getDescriptionForKey("date"));
        assertEquals("A specified format for the date parameter to parse. This should be a Java compliant format", reactor.getDescriptionForKey("format"));
        assertEquals("A default value to use for null columns", reactor.getDescriptionForKey("defaultValue"));
    
        reactor.keyValue.remove("date");
        nm = reactor.execute();
        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        assertInstanceOf(SemossDate.class, nm.getValue());
    }
}
