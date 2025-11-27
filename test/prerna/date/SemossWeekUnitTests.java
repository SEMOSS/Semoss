package prerna.date;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SemossWeekUnitTests {
    SemossWeek reactor;

    @Test
    void stringWeeks() {
        reactor = new SemossWeek("01");

        assertEquals(01, reactor.getNumWeeks());
    }

    @Test
    void intWeeks() {
        reactor = new SemossWeek(52);

        assertEquals(52, reactor.getNumWeeks());
    }
}
