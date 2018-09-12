package prerna.sablecc2.reactor.panel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SetPanelViewReactor extends AbstractInsightPanelReactor {
	
	public SetPanelViewReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.PANEL_VIEW_KEY.getKey(), ReactorKeysEnum.PANEL_VIEW_OPTIONS_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		if(insightPanel == null) {
			throw new IllegalArgumentException("Could not find the insight panel to set the view");
		}
		
		// get the view and options
		String view = getPanelView();
		String viewOptions = getPanelViewOptions();
		
		// if the view and current view are the same
		// and the view options is empty
		// keep the same view options that is on the panel
		boolean overrideView = true;
		String currentView = insightPanel.getPanelView();
		if(view.equals(currentView)) {
			if(viewOptions == null || viewOptions.isEmpty()) {
				overrideView = false;
			}
		}
		
		// set the new view
		insightPanel.setPanelView(view);
		if(overrideView) {
			insightPanel.setPanelViewOptions(viewOptions);
		}
		
		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("panelId", insightPanel.getPanelId());
		returnMap.put("view", view);
		// grab from panel since we do not know if we changed it or not
		returnMap.put("options", insightPanel.getPanelViewOptions());
		return new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PANEL_VIEW);
	}

	private String getPanelView() {
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

	private String getPanelViewOptions() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[2]);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return Utility.decodeURIComponent(genericReactorGrs.get(0).toString());
		}

		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
		if(strNouns != null && !strNouns.isEmpty()) {
			if(strNouns.size() > 1) {
				return Utility.decodeURIComponent(strNouns.get(1).getValue().toString());
			} else if(this.store.getNoun(keysToGet[1]) != null){
				// only return a valid view options at index 0 if and only if
				// the panel view is not set at index 0
				return Utility.decodeURIComponent(strNouns.get(0).getValue().toString());
			}
		}

		return null;
	}

}
