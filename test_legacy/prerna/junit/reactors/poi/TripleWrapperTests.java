package prerna.junit.reactors.poi;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.poi.main.TripleWrapper;

public class TripleWrapperTests {

	@Test
	public void testToString() {
		TripleWrapper tw = new TripleWrapper();
		tw.setObj1("1");
		tw.setPred("2");
		tw.setObj2("3");
		tw.setDocName("docName");
		tw.setSentence("sentence");
		tw.setObj1Count(11);
		tw.setPredCount(22);
		tw.setObj2Count(33);
		assertEquals("1>2>3;\nNA>NA>NA\n", tw.toString());
	}
}
