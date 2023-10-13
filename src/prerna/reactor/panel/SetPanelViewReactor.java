package prerna.reactor.panel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;
import prerna.util.insight.InsightUtility;

public class SetPanelViewReactor extends AbstractInsightPanelReactor {
	
	private static final Logger logger = LogManager.getLogger(SetPanelViewReactor.class); 
	private static Gson GSON = GsonUtility.getDefaultGson();
	
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
		Map<String, Object> viewOptionsMap = new HashMap<String, Object>();
		if(viewOptions != null && !viewOptions.isEmpty()) {
			try {
				viewOptionsMap = GSON.fromJson(viewOptions, Map.class);
			} catch(JsonSyntaxException e) {
				throw new SemossPixelException(new NounMetadata("Panel view is not in a valid JSON format after decoding", 
						PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
		}
		// set the new view
		insightPanel.setPanelView(view);
		insightPanel.appendPanelViewOptions(view, viewOptionsMap);
		Map<String, String> returnMap = new HashMap<String, String>();
		returnMap.put("panelId", insightPanel.getPanelId());
		returnMap.put("view", view);
		// grab the options for this view
		returnMap.put("options", insightPanel.getPanelActiveViewOptions());
		if(view.equalsIgnoreCase("text-editor")) {
			String renderedViewOptions = InsightUtility.recalculateHtmlViews(this.insight, insightPanel);
			returnMap.put("renderedOptions", renderedViewOptions);
		}
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
