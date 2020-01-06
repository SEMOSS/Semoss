package prerna.sablecc2.reactor.sheet;

import java.util.Map;

import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class CloseSheetReactor extends AbstractReactor {
	
	public CloseSheetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.SHEET.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// first input is the name of the sheet
		String sheetId = this.curRow.get(0).toString();
		InsightSheet sheetToDelete = this.insight.getInsightSheet(sheetId);
		if(sheetToDelete == null) {
			throw new IllegalArgumentException("Could not find sheet with id = " + sheetId + " to close.");
		}
		Map<String, InsightPanel> panels = this.insight.getInsightPanels();
		for(String key : panels.keySet()) {
			if(panels.get(key).getSheetId().equals(sheetId)) {
				throw new IllegalArgumentException("Cannot close sheet while it has open panels");
			}
		}
		return new NounMetadata(sheetId, PixelDataType.CONST_STRING, PixelOperationType.SHEET_CLOSE);
	}
}
