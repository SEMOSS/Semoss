package prerna.sablecc2.reactor.insights;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DropInsightReactor extends AbstractInsightReactor{

	private static final String CLASS_NAME = DropInsightReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		logger.info("Drop insight with id = " + this.insight.getInsightId());
		this.insight.dropWorkspaceCache();
		return new NounMetadata(true, PixelDataType.DROP_INSIGHT, PixelOperationType.DROP_INSIGHT);
	}
}
