package prerna.reactor.panel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CloneReactor extends AbstractInsightPanelReactor {
	
	public CloneReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.CLONE_PANEL_KEY.getKey(), ReactorKeysEnum.SHEET.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get the insight panel
		InsightPanel existingPanel = getInsightPanel();

		// get the new panel id
		String newId = getClonePanelId();
		// am i cloning in the same sheet, or cloning in a new sheet?
		String sheetId = this.keyValue.get(this.keysToGet[2]);
		if(sheetId == null) {
			sheetId = existingPanel.getSheetId();
		}
		if(this.insight.getInsightSheet(sheetId) == null) {
			// need to ad a new insight sheet
			InsightSheet newSheet = new InsightSheet(sheetId);
			this.insight.addNewInsightSheet(newSheet);
		}
		// make the new panel
		InsightPanel newClonePanel = new InsightPanel(newId, sheetId);
		newClonePanel.clone(existingPanel);
		
		// remember to add the new panel into the insight
		this.insight.addNewInsightPanel(newClonePanel);
		
		Map<String, InsightPanel> cloneMap = new HashMap<String, InsightPanel>();
		cloneMap.put("original", existingPanel);
		cloneMap.put("clone", newClonePanel);
		// return the new panel
		return new NounMetadata(cloneMap, PixelDataType.PANEL_CLONE_MAP, PixelOperationType.PANEL_CLONE);
	}
	
	private String getClonePanelId() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[1]);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return genericReactorGrs.get(0).toString();
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		// try if it is a string, int, or double
		List<String> strValues = this.curRow.getAllStrValues();
		if(strValues != null && !strValues.isEmpty()) {
			if(strValues.size() == 2) {
				return strValues.get(1);
			}
			return strValues.get(0);
		}
		
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_INT);
		if(strNouns != null && !strNouns.isEmpty()) {
			if(strNouns.size() == 2) {
				return strNouns.get(1).getValue().toString();
			}
			return strNouns.get(0).getValue().toString();
		}
		
		// well, you are out of luck
		return null;
	}
}