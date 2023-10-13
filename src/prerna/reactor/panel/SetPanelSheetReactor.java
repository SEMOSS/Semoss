package prerna.reactor.panel;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetPanelSheetReactor extends AbstractInsightPanelReactor {
	
	public SetPanelSheetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.SHEET.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		// get the ornaments that come as a map
		String panelSheet = getPanelSheet();
		if(panelSheet == null) {
			throw new IllegalArgumentException("Must pass in a sheet id for the insight");
		}
		if(this.insight.getInsightSheet(panelSheet) == null) {
			throw new IllegalArgumentException("Sheet id = " + panelSheet + " does not exist");
		}
		// merge the map options
		insightPanel.setSheetId(panelSheet);
		return new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL_LABEL);
	}
	
	private String getPanelSheet() {
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
		
		return null;
	}
	
}
