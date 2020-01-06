package prerna.sablecc2.reactor.sheet;

import prerna.om.InsightSheet;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class SheetReactor extends AbstractReactor {
	
	public SheetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.SHEET.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// first input is the name of the panel
		String sheetId = this.keyValue.get(this.keysToGet[0]);
		InsightSheet insightSheet = this.insight.getInsightSheet(sheetId);
		if(insightSheet == null) {
			throw new NullPointerException("Sheet Id " + sheetId + " does not exist");
		}
		NounMetadata noun = new NounMetadata(insightSheet, PixelDataType.SHEET, PixelOperationType.SHEET);
		return noun;
	}
}
