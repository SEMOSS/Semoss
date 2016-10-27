package prerna.sablecc.expressions.r;

import java.util.Map;

public class RMinReactor extends RBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	
	public RMinReactor() {
		this.setMathRoutine("min");
	}

	@Override
	public Map<String, Object> getColumnDataMap() {
		return getBaseColumnDataMap("Min");
	}
}
