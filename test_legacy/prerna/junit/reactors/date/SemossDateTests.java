package prerna.junit.reactors.date;

import static org.junit.Assert.assertEquals;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

import org.junit.Test;

import prerna.date.SemossDate;

public class SemossDateTests {

	@Test
	public void testSemossDate() {
		LocalDate ld = LocalDate.of(2022, 1, 2);
		Date d = Date.from(ld.atStartOfDay(ZoneId.systemDefault()).toInstant());
		SemossDate semossDate = new SemossDate(d);
		
		assertEquals("2022-01-02", semossDate.toString());
	}
	
}
