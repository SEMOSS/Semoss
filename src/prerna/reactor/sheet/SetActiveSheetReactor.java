package prerna.reactor.sheet;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import prerna.om.InsightSheet;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

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
		
		// loop through the sheets and their labels
		// if we find the sheet label that matches the input
		// store its id
		// if the label ends up being duplicative
		// throw an error
		Set<String> sheetLabels = new HashSet<>();
		String actualSheetId = null;
		for(String sheetId : insightSheets.keySet()) {
			InsightSheet sheet = insightSheets.get(sheetId);
			String sheetLabel = sheet.getSheetLabel();
			
			if(sheetLabels.contains(sheetLabel)) {
				if(sheetLabel.equals(possibleSheetId)) {
					// not unique
					throw new IllegalArgumentException("Please make sure the sheets have unique identifiers.");
				}
			} else {
				sheetLabels.add(sheetLabel);
			}
			
			if(sheetLabel.equals(possibleSheetId)) {
				// grab the sheetId
				// if label name is reused - it would throw error above
				actualSheetId = sheetId;
			}
		}
		
		if(actualSheetId != null) {
			return new NounMetadata(actualSheetId, PixelDataType.CONST_STRING, PixelOperationType.MOVE_SHEET);
		}
		
		throw new IllegalArgumentException("Count not find sheet with id or label = " + possibleSheetId);
	}

}
