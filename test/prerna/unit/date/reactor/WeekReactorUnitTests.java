package prerna.unit.date.reactor;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.date.SemossWeek;
import prerna.date.reactor.WeekReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class WeekReactorUnitTests {
    private WeekReactor reactor;
	private Map<String, String> keyValues;

    @BeforeEach
	void setup() {
		reactor = new WeekReactor();
		keyValues = reactor.keyValue;
	}

    @Test
    void getWeek() {
        keyValues.put("weeks", "52");
        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_WEEK, nm.getNounType());
        SemossWeek week = (SemossWeek) nm.getValue();
        assertEquals(52, week.getNumWeeks());
    }
}
