package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.insight.InsightUtility;

public class ClearInsightReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		NounMetadata newNoun = InsightUtility.clearInsight(this.insight);
		return newNoun;
	}
}
