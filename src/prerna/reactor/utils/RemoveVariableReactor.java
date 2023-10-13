package prerna.reactor.utils;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class RemoveVariableReactor extends AbstractReactor {
	
	private static final String DROP_NOW_KEY = "dropNow";
	
	public RemoveVariableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey(), DROP_NOW_KEY};
	}

	@Override
	public NounMetadata execute() {
		String variableName = this.curRow.get(0).toString();
		if(dropNow()) {
			return InsightUtility.removeVaraible(this.insight.getVarStore(), variableName);
		}
		return new NounMetadata(variableName, PixelDataType.REMOVE_VARIABLE, PixelOperationType.FORCE_SAVE_DATA_TRANSFORMATION);
	}
	
	/**
	 * Determine if we should remove right away or during the stream
	 * @return
	 */
	protected boolean dropNow() {
		if(this.curRow.size() > 1) {
			return Boolean.parseBoolean(this.curRow.get(1).toString());
		}
		return false;
	}
}
