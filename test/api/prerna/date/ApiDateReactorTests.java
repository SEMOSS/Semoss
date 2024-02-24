package api.prerna.date;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import api.ApiSemossTestUtils;
import api.BaseSemossApiTests;
import prerna.date.reactor.DateReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ApiDateReactorTests extends BaseSemossApiTests {
	
	@Test
	public void getDate() {
		String pixel = ApiSemossTestUtils.buildPixelCall(DateReactor.class, "date", "3/19/2022");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		String date = nm.getValue().toString();
		assertEquals("3/19/2022", date);
	}

}
