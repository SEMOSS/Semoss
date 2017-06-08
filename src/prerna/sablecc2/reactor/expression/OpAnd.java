package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpAnd extends OpBasic {

	@Override
	protected NounMetadata evaluate(Object[] values) {
		boolean result = eval(values);
		return new NounMetadata(result, PkslDataTypes.BOOLEAN);
	}
	
	public static boolean eval(Object...values) {
		boolean result = true;
		for (Object booleanValue : values) {
			// need all values to be true
			// in order to return true
			if(! (boolean) booleanValue) {
				result = false;
				break;
			}
		}
		return result;
	}

	@Override
	public String getReturnType() {
		// TODO Auto-generated method stub
		return "boolean";
	}
}
