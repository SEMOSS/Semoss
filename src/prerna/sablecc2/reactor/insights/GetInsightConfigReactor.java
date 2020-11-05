package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

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
