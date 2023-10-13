package prerna.reactor.expression;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpNotEmpty extends OpBasic {
	
	public OpNotEmpty() {
		this.keysToGet = new String[]{ReactorKeysEnum.VALUES.getKey()};
	}

	@Override
	protected NounMetadata evaluate(Object[] values) {
		boolean notEmpty = false;
		if(values != null && values.length > 0) {
			notEmpty = true;
		}
		return new NounMetadata(notEmpty, PixelDataType.BOOLEAN);
	}
	
	@Override
	public String getReturnType() {
		return "boolean";
	}

}
