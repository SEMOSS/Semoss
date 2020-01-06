package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class SetInsightConfigReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		NounMetadata noun = this.curRow.getNoun(0);
		// this is just an echo, where i send it back to the FE
		return new NounMetadata(noun.getValue(), noun.getNounType(), PixelOperationType.INSIGHT_CONFIG);
	}

}
