package api.prerna.date;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import api.ApiTestUtils;
import api.ApiTests;
import prerna.date.reactor.DateReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ApiDateReactorTests extends ApiTests {
	
	@Test
	public void getDate() {
		String pixel = ApiTestUtils.buildPixelCall(DateReactor.class, "date", "3/19/2022");
		NounMetadata nm = ApiTestUtils.processPixel(pixel);
		String date = nm.getValue().toString();
		assertEquals("3/19/2022", date);
	}

}
