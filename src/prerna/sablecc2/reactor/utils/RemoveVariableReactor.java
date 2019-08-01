package prerna.sablecc2.reactor.utils;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.insight.InsightUtility;

public class RemoveVariableReactor extends AbstractInsightReactor {
	
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
		return new NounMetadata(variableName, PixelDataType.REMOVE_VARIABLE);
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
