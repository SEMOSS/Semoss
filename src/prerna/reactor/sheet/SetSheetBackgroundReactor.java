package prerna.reactor.sheet;

import java.util.List;

import prerna.om.InsightSheet;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetSheetBackgroundReactor extends AbstractSheetReactor {

	public SetSheetBackgroundReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.SHEET.getKey(), ReactorKeysEnum.SHEET_LABEL_KEY.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		InsightSheet insightSheet = getInsightSheet();
		// get the background
		String sheetBackground = getSheetBackground();
		// merge the map options
		insightSheet.setBackgroundColor(sheetBackground);
		return new NounMetadata(insightSheet, PixelDataType.SHEET, PixelOperationType.SHEET_BACKGROUND);
	}

	private String getSheetBackground() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[1]);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return genericReactorGrs.get(0).toString();
		}

		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
		if(strNouns != null && !strNouns.isEmpty()) {
			return strNouns.get(0).getValue().toString();
		}
		
		return "";
	}
}
