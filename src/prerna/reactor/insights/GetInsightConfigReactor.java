package prerna.reactor.insights;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetInsightConfigReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		NounMetadata insightConfig = this.insight.getVarStore().get(SetInsightConfigReactor.INSIGHT_CONFIG);
		if(insightConfig == null) {
			return new NounMetadata(null, PixelDataType.NULL_VALUE);
		}
		return insightConfig;
	}

}
