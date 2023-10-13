package prerna.reactor.expression.filter;

import prerna.reactor.expression.OpBasic;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpOr extends OpBasic {
	
	public OpOr() {
		this.keysToGet = new String[]{ReactorKeysEnum.VALUES.getKey()};
	}

	@Override
	protected NounMetadata evaluate(Object[] values) {
		boolean result = eval(values);
		return new NounMetadata(result, PixelDataType.BOOLEAN);
	}
	
	public boolean eval(Object... values) {
		boolean result = false;
		for (Object booleanValue : values) {
			// need only 1 value to be true
			// in order to return true
			if((boolean) booleanValue) {
				result = true;
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
