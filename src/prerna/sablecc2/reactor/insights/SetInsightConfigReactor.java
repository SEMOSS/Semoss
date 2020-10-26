package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class SetInsightConfigReactor extends AbstractReactor {

	public static final String INSIGHT_CONFIG = "$INSIGHT_CONFIG";
	
	@Override
	public NounMetadata execute() {
		NounMetadata noun = this.curRow.getNoun(0);
		// this is just an echo, where i send it back to the FE
		NounMetadata data = new NounMetadata(noun.getValue(), noun.getNounType(), PixelOperationType.INSIGHT_CONFIG);
		this.insight.getVarStore().put(INSIGHT_CONFIG, data);
		return data;
	}

}
