package prerna.sablecc2.reactor.sheet;

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
		// exercise
		// see if this is a valid sheet id
		// or try to find the sheet label for it
		// otherwise, throw an error that the sheet doesn't exist...
		return new NounMetadata(possibleSheetId, PixelDataType.CONST_STRING, PixelOperationType.MOVE_SHEET);
	}

}
