package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class InsightHandleReactor extends AbstractReactor {
	
	/**
	 * This is just an echo back for the default handle that is backing this insight
	 */
	
	@Override
	public NounMetadata execute() {
		String encodedValue = this.curRow.get(0).toString();
		String decodedText = Utility.decodeURIComponent(encodedValue);
		return new NounMetadata(decodedText, PixelDataType.CONST_STRING, PixelOperationType.INSIGHT_HANDLE);
	}
}
