package prerna.sablecc2.reactor.utils;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;

public class RemoveFrameReactor extends AbstractInsightReactor {
	
	public RemoveFrameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String variableName = this.curRow.get(0).toString();
		NounMetadata noun = new NounMetadata(variableName, PixelDataType.REMOVE_VARIABLE, PixelOperationType.REMOVE_FRAME);

		// make sure it is a valid removal
		NounMetadata var = this.insight.getVarStore().get(variableName);
		if(var == null) {
			noun.addAdditionalReturn(new NounMetadata("Could not find variable to remove", PixelDataType.CONST_STRING, PixelOperationType.WARNING));
		} else if(var.getNounType() != PixelDataType.FRAME) {
			noun.addAdditionalReturn(new NounMetadata("Trying to remove a variable that is not a frame", PixelDataType.CONST_STRING, PixelOperationType.WARNING));
		}
		
		return noun;
	}

}
