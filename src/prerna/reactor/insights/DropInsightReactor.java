package prerna.reactor.insights;

import org.apache.logging.log4j.Logger;

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
		// this only returns the boolean
		// called from the PixelStreamUtility, the following method
		// {@link InsightUtility.dropInsight(Insight insight)}
		// actually does the cleanup
		return new NounMetadata(true, PixelDataType.DROP_INSIGHT, PixelOperationType.DROP_INSIGHT);
	}
}
