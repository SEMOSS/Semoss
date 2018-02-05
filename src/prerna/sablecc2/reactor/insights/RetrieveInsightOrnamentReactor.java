package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class RetrieveInsightOrnamentReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		return new NounMetadata(this.insight.getInsightOrnament(), PixelDataType.MAP, PixelOperationType.INSIGHT_ORNAMENT);
	}

}
