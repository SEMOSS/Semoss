package prerna.reactor.sheet;

import java.util.List;

import prerna.om.InsightSheet;
import prerna.reactor.AbstractReactor;
//import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public abstract class AbstractSheetReactor extends AbstractReactor {

	protected InsightSheet getInsightSheet() {
		// passed in directly as sheet
		GenRowStruct genericReactorGrs = this.store.getNoun(ReactorKeysEnum.SHEET.getKey());
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			NounMetadata noun = genericReactorGrs.getNoun(0);
			PixelDataType nounType = noun.getNounType();
			if(nounType == PixelDataType.SHEET) {
				return (InsightSheet) noun.getValue();
			} else if(nounType == PixelDataType.COLUMN || nounType == PixelDataType.CONST_STRING) {
				String sheetId = noun.getValue().toString();
				return this.insight.getInsightSheet(sheetId);
			}
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PixelDataType.SHEET);
		if(panelNouns != null && !panelNouns.isEmpty()) {
			return (InsightSheet) panelNouns.get(0).getValue();
		}
		
		// see if string or column passed in
		List<String> strInputs = this.curRow.getAllStrValues();
		if(strInputs != null && !strInputs.isEmpty()) {
			for(String sheetId : strInputs) {
				InsightSheet sheet = this.insight.getInsightSheet(sheetId);
				if(sheet != null) {
					return sheet;
				}
			}
		}
		
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_INT);
		if(strNouns != null && !strNouns.isEmpty()) {
			return this.insight.getInsightSheet(strNouns.get(0).getValue().toString());
		}
		
		// well, you are out of luck
		return null;
	}
	
}
