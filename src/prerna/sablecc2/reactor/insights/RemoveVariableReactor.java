package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RemoveVariableReactor extends AbstractInsightReactor{
	
	public RemoveVariableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String variableName = this.curRow.get(0).toString();
		NounMetadata noun = this.planner.removeVariable(variableName);
		boolean success = true;
		if(noun == null) {
			success = false;
		}
		return new NounMetadata(success, PixelDataType.BOOLEAN);
	}

}
