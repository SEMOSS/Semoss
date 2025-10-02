package prerna.date;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SemossMonthUnitTests {
    SemossMonth reactor;

    @Test
    void stringMonth() {
        reactor = new SemossMonth("01");

        assertEquals(01, reactor.getNumMonths());
    }

    @Test
    void intMonth() {
        reactor = new SemossMonth(12);

        assertEquals(12, reactor.getNumMonths());
    }
}
