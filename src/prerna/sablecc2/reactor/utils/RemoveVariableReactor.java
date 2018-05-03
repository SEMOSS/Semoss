package prerna.sablecc2.reactor.utils;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;

public class RemoveVariableReactor extends AbstractInsightReactor{
	
	public RemoveVariableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String variableName = this.curRow.get(0).toString();
		return new NounMetadata(variableName, PixelDataType.REMOVE_VARIABLE);
	}

}
