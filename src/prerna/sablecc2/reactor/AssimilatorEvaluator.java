package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * This is just a internal class
 * so that when we compile to execute the assimilator
 * we can have a method to call based on the super that is 
 * assigned to the new class
 *
 */
public abstract class AssimilatorEvaluator extends AbstractReactor {

	public Map<String, Object> vars = new HashMap<>();
	public boolean containsStringValue = false;
	public boolean allIntValue = true;

	public AssimilatorEvaluator() {
		
	}
	
	@Override
	public NounMetadata execute() {
		NounMetadata noun = null;
		Object retVal = getExpressionValue();
		if(containsStringValue) {
			noun = new NounMetadata(retVal.toString(), PixelDataType.CONST_STRING);
		} else if(allIntValue) {
			Number result = (Number) retVal;
			if(result.doubleValue() == Math.rint(result.doubleValue())) {
				noun = new NounMetadata( ((Number) retVal).intValue(), PixelDataType.CONST_INT);
			} else {
				// not a valid integer
				// return as a double
				noun = new NounMetadata( ((Number) retVal).doubleValue(), PixelDataType.CONST_DECIMAL);
			}
		} else {
			noun = new NounMetadata( ((Number) retVal).doubleValue(), PixelDataType.CONST_DECIMAL);
		}

		return noun;
	}
	
	/**
	 * Method that return the evaluation of 
	 * the signature
	 * @return
	 */
	public abstract Object getExpressionValue();
	
	public void setVars(Map<String, Object> vars) {
		this.vars = vars;
	}
	
	public void setVar(String key, Object value) {
		this.vars.put(key, value);
	}
	
	public Object getVar(String key) {
		return this.vars.get(key);
	}
}