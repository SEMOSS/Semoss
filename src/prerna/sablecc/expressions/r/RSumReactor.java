package prerna.sablecc.expressions.r;

import java.util.Map;

public class RSumReactor extends RBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	
	public RSumReactor() {
		this.setMathRoutine("sum");
	}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		return getBaseColumnDataMap("Sum");
	}
}
