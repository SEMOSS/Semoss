package prerna.date;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SemossYearUnitTests {
    SemossYear reactor;

    @Test
    void stringYear() {
        reactor = new SemossYear("2000");

        assertEquals(2000, reactor.getNumYears());
    }

    @Test
    void intYear() {
        reactor = new SemossYear(2025);

        assertEquals(2025, reactor.getNumYears());
    }
}
