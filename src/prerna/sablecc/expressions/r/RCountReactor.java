package prerna.sablecc.expressions.r;

import java.util.Map;

public class RCountReactor extends RBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	
	public RCountReactor() {
		this.setMathRoutine("uniqueN");
	}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		return getBaseColumnDataMap("Count");
	}
	
}
