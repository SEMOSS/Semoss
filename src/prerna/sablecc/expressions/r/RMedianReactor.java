package prerna.sablecc.expressions.r;

import java.util.Map;

public class RMedianReactor extends RBasicMathReactor {
	
	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	
	public RMedianReactor() {
		this.setMathRoutine("median");
	}

	@Override
	public Map<String, Object> getColumnDataMap() {
		return getBaseColumnDataMap("Median");
	}
}
