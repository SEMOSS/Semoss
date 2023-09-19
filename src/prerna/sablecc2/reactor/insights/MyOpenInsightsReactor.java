package prerna.sablecc2.reactor.insights;

import java.util.Set;

import prerna.om.InsightStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class MyOpenInsightsReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		Set<String> insightIdsForSesh = InsightStore.getInstance().getInsightIDsForSession(getSessionId());
		return new NounMetadata(insightIdsForSesh, PixelDataType.VECTOR);
	}
}
