package prerna.reactor.insights;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetAppConfigReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		NounMetadata appConfig = this.insight.getVarStore().get(SetAppConfigReactor.APP_CONFIG);
		if(appConfig == null) {
			return new NounMetadata(null, PixelDataType.NULL_VALUE);
		}
		return appConfig;
	}

}
