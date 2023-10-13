package prerna.reactor.insights;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RetrieveInsightOrnamentReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		return new NounMetadata(this.insight.getInsightOrnament(), PixelDataType.MAP, PixelOperationType.INSIGHT_ORNAMENT);
	}

}
