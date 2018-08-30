package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DropInsightReactor extends AbstractInsightReactor{

	@Override
	public NounMetadata execute() {
		return new NounMetadata(true, PixelDataType.DROP_INSIGHT, PixelOperationType.DROP_INSIGHT);
	}
}
