package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpLen extends OpBasic {
	
	public OpLen() {
		this.keysToGet = new String[]{ReactorKeysEnum.VALUES.getKey()};
	}

	@Override
	protected NounMetadata evaluate(Object[] values) {
		return new NounMetadata(values.length, PixelDataType.CONST_INT);
	}
	
	@Override
	public String getReturnType() {
		return "boolean";
	}

}
