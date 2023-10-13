package prerna.reactor.utils;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class RemoveFrameReactor extends RemoveVariableReactor {
	
	private static final String DROP_NOW_KEY = "dropNow";

	public RemoveFrameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey(), DROP_NOW_KEY};
	}

	@Override
	public NounMetadata execute() {
		String variableName = this.curRow.get(0).toString();
		if(dropNow()) {
			return InsightUtility.removeFrameVaraible(this.insight.getVarStore(), variableName);
		}
		
		NounMetadata noun = new NounMetadata(variableName, PixelDataType.REMOVE_VARIABLE, PixelOperationType.REMOVE_FRAME, PixelOperationType.FORCE_SAVE_DATA_TRANSFORMATION);

		// make sure it is a valid removal
		NounMetadata var = this.insight.getVarStore().get(variableName);
		if(var == null) {
			noun.addAdditionalReturn(NounMetadata.getWarningNounMessage("Could not find variable to remove"));
		} else if(var.getNounType() != PixelDataType.FRAME) {
			noun.addAdditionalReturn(NounMetadata.getWarningNounMessage("Trying to remove a variable that is not a frame"));
		}
		
		return noun;
	}
}
