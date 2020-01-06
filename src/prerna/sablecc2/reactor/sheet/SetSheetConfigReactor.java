package prerna.sablecc2.reactor.sheet;

import java.util.List;
import java.util.Map;

import prerna.om.InsightSheet;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetSheetConfigReactor extends AbstractSheetReactor {

	public SetSheetConfigReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.SHEET.getKey(), ReactorKeysEnum.SHEET_CONFIG_KEY.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		InsightSheet insightSheet = getInsightSheet();
		// get the label
		Map<String, String> sheetConfig = getSheetConfig();
		// merge the map options
		if(sheetConfig.containsKey("sheetLabel")) {
			insightSheet.setSheetLabel(sheetConfig.get("sheetLabel"));
		}
		if(sheetConfig.containsKey("sheetBackground")) {
			insightSheet.setSheetBackground(sheetConfig.get("sheetBackground"));
		}
		
		return new NounMetadata(insightSheet, PixelDataType.SHEET, PixelOperationType.SHEET_CONFIG);
	}

	private Map<String, String> getSheetConfig() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[1]);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (Map<String, String>) genericReactorGrs.get(0);
		}

		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(strNouns != null && !strNouns.isEmpty()) {
			return (Map<String, String>) strNouns.get(0).getValue();
		}
		
		return null;
	}
}
