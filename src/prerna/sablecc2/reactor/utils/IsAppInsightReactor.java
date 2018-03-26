package prerna.sablecc2.reactor.utils;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class IsAppInsightReactor extends AbstractReactor {

	/**
	 * See if insight has a CSV / Excel file that was user uploaded (DnD)
	 */
	
	@Override
	public NounMetadata execute() {
		boolean isAppInsight = insight.getFilesUsedInInsight() == null || insight.getFilesUsedInInsight().isEmpty();
		return new NounMetadata(isAppInsight, PixelDataType.BOOLEAN);
	}

}
