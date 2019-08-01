package prerna.sablecc2.reactor.utils;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.insight.InsightUtility;

public class RemoveFrameReactor extends AbstractInsightReactor {
	
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
	
	/**
	 * Determine if we should remove right away or during the stream
	 * @return
	 */
	private boolean dropNow() {
		if(this.curRow.size() > 1) {
			return Boolean.parseBoolean(this.curRow.get(1).toString());
		}
		return false;
	}
}
