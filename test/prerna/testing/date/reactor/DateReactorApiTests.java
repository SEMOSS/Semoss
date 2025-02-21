package prerna.testing.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import prerna.date.reactor.DateReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.AbstractBaseSemossApiTests;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateReactorApiTests extends AbstractBaseSemossApiTests {
	
	@Test
	public void getDate() {
		String pixel = ApiSemossTestUtils.buildPixelCall(DateReactor.class, "date", "2022-03-19");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		String date = nm.getValue().toString();
		assertEquals("2022-03-19", date);
	}
	
	@Test
	public void getDate2() {
		// sanity check to make sure multiple tests run fine :)
		String pixel = ApiSemossTestUtils.buildPixelCall(DateReactor.class, "date", "2022-03-19");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		String date = nm.getValue().toString();
		assertEquals("2022-03-19", date);
	}
	
	 @Test 
	 public void getDateWithCustomFormat() {
	     String pixel = ApiSemossTestUtils.buildPixelCall(DateReactor.class, "date", "2022-03-19", "format", "dd/MM/yyyy");
	     NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
	     String date = nm.getValue().toString();
	     assertEquals("19/03/2022", date);
	  }
	 
	  @Test
	  public void getDateWithNoInput() {
	      String pixel = ApiSemossTestUtils.buildPixelCall(DateReactor.class);
	      NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
	      String date = nm.getValue().toString();
	      String today = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
	      assertEquals(today, date);
	   }

}
