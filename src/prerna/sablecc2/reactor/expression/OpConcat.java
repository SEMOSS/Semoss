package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpConcat extends OpBasic {

	public OpConcat() {
		this.operation="concat";
		this.keysToGet = new String[]{ReactorKeysEnum.ARRAY.getKey()};
	}

	@Override
	protected NounMetadata evaluate(Object[] values) {
		StringBuilder builder = new StringBuilder();
		if (values.length != 0) { 
			for(int i = 0; i < values.length; i++){ 
				builder.append(values[i].toString());
			}
		}
		NounMetadata noun = new NounMetadata(builder.toString(), PixelDataType.CONST_STRING);
		return noun;
	}

	@Override
	public String getReturnType() {
		return "String";
	}

}
