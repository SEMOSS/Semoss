package prerna.reactor.insights;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetCurrentInsightIdReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		return new NounMetadata(this.insight.getInsightId(), PixelDataType.CONST_STRING);
	}

}
