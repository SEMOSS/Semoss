package prerna.sablecc.expressions.r;

import java.util.Map;

public class RMaxReactor extends RBasicMathReactor {

	/*
	 * Only need to set the Math Routine
	 * Everything else is handled by inheritance
	 */
	
	public RMaxReactor() {
		this.setMathRoutine("max");
	}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		return getBaseColumnDataMap("Max");
	}

}
