package prerna.reactor.insights;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetInsightGoldenLayoutReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		NounMetadata noun = this.curRow.getNoun(0);
		// this is just an echo, where i send it back to the FE
		return new NounMetadata(noun.getValue(), noun.getNounType(), PixelOperationType.GOLDEN_LAYOUT);
	}

}
