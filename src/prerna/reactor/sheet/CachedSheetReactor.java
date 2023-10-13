package prerna.reactor.sheet;

import prerna.om.InsightSheet;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CachedSheetReactor extends AbstractReactor {
	
	/**
	 * This code is the same as the Panel Reactor
	 * But it has a different op type
	 * 
	 * It is only intended to be used to simplify the 
	 * cached insight recipe into a single call to get the panel
	 * state instead of multiple calls for each portion of the insight
	 * 
	 */
	
	public CachedSheetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.SHEET.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// first input is the name of the sheet
		organizeKeys();
		String sheetId = this.keyValue.get(this.keysToGet[0]);
		InsightSheet insightSheet = this.insight.getInsightSheet(sheetId);
		if(insightSheet == null) {
			throw new NullPointerException("Sheet Id " + sheetId + " does not exist");
		}
		NounMetadata noun = new NounMetadata(insightSheet, PixelDataType.SHEET, PixelOperationType.CACHED_SHEET);
		return noun;
	}
}
