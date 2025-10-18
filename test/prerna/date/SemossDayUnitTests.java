package prerna.date;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SemossDayUnitTests {
    SemossDay reactor;

    @Test
    void stringNumDays() {
        reactor = new SemossDay("01");

        assertEquals(01, reactor.getNumDays());
    }
    
    @Test
    void intNumDays() {
        reactor = new SemossDay(30);

        assertEquals(30, reactor.getNumDays());
    }
}
