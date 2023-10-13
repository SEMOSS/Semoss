package prerna.reactor.insights.save;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetInsightCachedDateTimeReactor extends AbstractReactor {

	public GetInsightCachedDateTimeReactor() {
		this.keysToGet = new String[]{};
	}
	
	@Override
	public NounMetadata execute() {
		if(!this.insight.isSavedInsight()) {
			throw new IllegalArgumentException("The insight must be saved in order to have a cached version");
		}
		
		if(this.insight.getCachedDateTime() != null) {
			return new NounMetadata(this.insight.getCachedDateTime().toString(), PixelDataType.CONST_STRING);
		}
		
		return new NounMetadata("This instance of the insight is not cached", PixelDataType.CONST_STRING);
	}

}
