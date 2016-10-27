package prerna.sablecc.expressions.r;

import java.util.Map;

public class RStandardDeviationReactor extends RBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	
	public RStandardDeviationReactor() {
		this.setMathRoutine("sd");
	}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		return getBaseColumnDataMap("StandardDeviation");
	}

}