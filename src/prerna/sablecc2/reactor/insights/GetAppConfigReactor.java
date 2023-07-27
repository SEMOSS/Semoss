package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

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
