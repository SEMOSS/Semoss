package prerna.reactor.insights;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetAppConfigReactor extends AbstractReactor {

	public static final String APP_CONFIG = "$APP_CONFIG";
	
	@Override
	public NounMetadata execute() {
		NounMetadata noun = this.curRow.getNoun(0);
		// this is just an echo, where i send it back to the FE
		NounMetadata data = new NounMetadata(noun.getValue(), noun.getNounType(), PixelOperationType.APP_CONFIG);
		this.insight.getVarStore().put(APP_CONFIG, data);
		return data;
	}

}
