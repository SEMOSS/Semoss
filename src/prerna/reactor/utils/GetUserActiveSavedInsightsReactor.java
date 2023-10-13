package prerna.reactor.utils;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetUserActiveSavedInsightsReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		if(this.insight.getUser() == null) {
			return new NounMetadata(null, PixelDataType.NULL_VALUE);
		}
		
		return new NounMetadata(this.insight.getUser().getOpenInsights(), PixelDataType.MAP);
	}

}
