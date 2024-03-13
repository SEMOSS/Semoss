package prerna.junit.reactors.math;

import org.junit.Test;

import prerna.math.StatisticsUtilityMethods;

public class StatisticsUtilityMethodsTests {
	
	@Test(expected = IllegalArgumentException.class)
	public void testQuartile() {
		StatisticsUtilityMethods.quartile(null, 0, false);
	}

}
