package prerna.testing.date.reactor;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import prerna.date.reactor.DayReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.date.SemossDay;


public class DayReactorApiTests extends AbstractBaseSemossApiTests{
	
	@Test
	public void getDay() {
		 String pixel = ApiSemossTestUtils.buildPixelCall(DayReactor.class, "days", "365");
		 NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		 SemossDay day = (SemossDay) nm.getValue();
		 assertEquals(365, day.getNumDays());
	}

}
