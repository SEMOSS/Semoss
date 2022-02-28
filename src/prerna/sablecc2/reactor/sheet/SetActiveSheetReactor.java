package prerna.sablecc2.reactor.sheet;

import java.util.HashMap;
import java.util.Map;

import prerna.om.InsightSheet;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class SetActiveSheetReactor extends AbstractReactor {

	// SetActiveSheet("1");
	// SetActiveSheet(sheet=["1"]);
	// SetActiveSheet(sheet=["Sheet1"]);
	// SetActiveSheet(sheet=["abc"]);
	
	public SetActiveSheetReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.SHEET.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		// user input
		String possibleSheetId = this.keyValue.get(this.keysToGet[0]);
		// sheetId -> insight sheet object
		Map<String, InsightSheet> insightSheets = this.insight.getInsightSheets();
		
		// first, check if the insight id is a valid sheet id
		if(insightSheets.containsKey(possibleSheetId)) {
			return new NounMetadata(possibleSheetId, PixelDataType.CONST_STRING, PixelOperationType.MOVE_SHEET);
		}
		
		// aggregate the sheet labels
		Map<String, Integer> sheetLabels = new HashMap<>();
		for(String sheetId : insightSheets.keySet()) {
			InsightSheet sheet = insightSheets.get(sheetId);
			String sheetLabel = sheet.getSheetLabel();
			
			if(sheetLabels.containsKey(sheetLabel)) {
				sheetLabels.put(sheetLabel, sheetLabels.get(sheetLabel)+1);
			} else {
				sheetLabels.put(sheetLabel, 1);
			}
		}
		
		if(sheetLabels.containsKey(possibleSheetId)) {
			// is this label unique?
			if(sheetLabels.get(possibleSheetId) == 1) {
				return new NounMetadata(possibleSheetId, PixelDataType.CONST_STRING, PixelOperationType.MOVE_SHEET);
			}
			// not unique
			throw new IllegalArgumentException("Please make sure the sheets have unique identifiers.");
		}
		
		throw new IllegalArgumentException("Count not find sheet with id or label = " + possibleSheetId);
	}

}
