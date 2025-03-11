package prerna.testing.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.junit.jupiter.api.Test;

import prerna.date.SemossDate;
import prerna.date.reactor.DateReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

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
		String pixel = ApiSemossTestUtils.buildPixelCall(DateReactor.class, "format", "dd/MM/yyyy");
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		SemossDate sd = (SemossDate) nm.getValue();
		assertTrue(sd.getFormattedDate().contains("/"));
		assertFalse(sd.getFormattedDate().contains("-"));
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
