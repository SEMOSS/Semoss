package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ClearInsightReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		// since we stream results in pixel web utility
		// we will just put a placeholder here to clear the insight
		return new NounMetadata(true, PixelDataType.CLEAR_INSIGHT, PixelOperationType.CLEAR_INSIGHT);
	}
}
