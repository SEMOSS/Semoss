package prerna.testing.date;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import prerna.date.reactor.DateReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.AbstractBaseSemossApiTests;

public class ApiDateReactorTests extends AbstractBaseSemossApiTests {
	
	@Test
	public void getDate() {
		String pixel = ApiSemossTestUtils.buildPixelCall(DateReactor.class, "date", "3/19/2022");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		String date = nm.getValue().toString();
		assertEquals("3/19/2022", date);
	}
	
	@Test
	public void getDate2() {
		// sanity check to make sure multiple tests run fine :)
		String pixel = ApiSemossTestUtils.buildPixelCall(DateReactor.class, "date", "3/19/2022");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		String date = nm.getValue().toString();
		assertEquals("3/19/2022", date);
	}

}
