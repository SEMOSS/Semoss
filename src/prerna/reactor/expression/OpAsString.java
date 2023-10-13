package prerna.reactor.expression;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpAsString extends OpReactor {
	
	public OpAsString() {
		this.keysToGet = new String[]{ReactorKeysEnum.VALUES.getKey()};
	}

	@Override
	public NounMetadata execute() {
		NounMetadata[] nouns = getValues();
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < nouns.length; i++) {
			if(i != 0) {
				builder.append(",");
			}
			builder.append(nouns[i].toString());
		}
		NounMetadata retNoun = new NounMetadata(builder.toString(), PixelDataType.CONST_STRING);
		return retNoun;
	}
	
	@Override
	public String getReturnType() {
		return "String";
	}
}
