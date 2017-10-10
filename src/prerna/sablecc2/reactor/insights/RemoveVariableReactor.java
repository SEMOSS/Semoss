package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;

public class RemoveVariableReactor extends AbstractInsightReactor{

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
