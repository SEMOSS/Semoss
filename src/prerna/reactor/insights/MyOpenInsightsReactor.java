package prerna.reactor.insights;

import java.util.Set;

import prerna.om.InsightStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MyOpenInsightsReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		Set<String> insightIdsForSesh = InsightStore.getInstance().getInsightIDsForSession(getSessionId());
		return new NounMetadata(insightIdsForSesh, PixelDataType.VECTOR);
	}
}
