package prerna.unit.date.reactor;

import static org.junit.Assert.assertEquals;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.date.SemossDay;
import prerna.date.reactor.DayReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DayReactorUnitTests {
    private DayReactor reactor;
	private Map<String, String> keyValues;

    @BeforeEach
	void setup() { 
		reactor = new DayReactor();
		keyValues = reactor.keyValue;
	}

    @Test
    void getDay() {
        keyValues.put("days", "365");
        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_DAY, nm.getNounType());
        SemossDay day = (SemossDay) nm.getValue();
        assertEquals(365, day.getNumDays());
    }
}