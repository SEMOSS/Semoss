package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.insight.InsightUtility;

public class ClearInsightReactor extends AbstractReactor {

	public ClearInsightReactor() {
		this.keysToGet = new String[] {"suppress"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		boolean suppress = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[0]));
		NounMetadata newNoun = InsightUtility.clearInsight(this.insight, suppress);
		return newNoun;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(this.keysToGet[0])) {
			return "Suppress the opType to be the default OPERATION";
		}
		return super.getDescriptionForKey(key);
	}
}
