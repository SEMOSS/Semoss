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

public class AddPanelConfigReactor extends AbstractInsightPanelReactor {
	
	// input keys for the map
	private static final String LABEL = "label";
	private static final String CONFIG = "config";

	public AddPanelConfigReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.CONFIG.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		// get the map
		Map<String, Object> mapInput = getMapInput();
		if(mapInput == null) {
			throw new IllegalArgumentException("Need to define the config input");
		}
		// config also has a short hand to set the panel label
		if(mapInput.containsKey(LABEL)) {
			insightPanel.setPanelLabel(mapInput.get(LABEL).toString());
		}
		Map<String, Object> config = (Map<String, Object>) mapInput.get(CONFIG);
		// default config if not passed in
		if (config == null) {
			config = new HashMap<String, Object>();
		}
		if (!config.containsKey("labelOverride")) {
			config.put("labelOverride", false);
		}  
		// merge the map options
		insightPanel.addConfig(config);
		return new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL_CONFIG);
	}

	private Map<String, Object> getMapInput() {
		// see if it was passed directly in with the lower case key config
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[1]);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (Map<String, Object>) genericReactorGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(panelNouns != null && !panelNouns.isEmpty()) {
			return (Map<String, Object>) panelNouns.get(0).getValue();
		}
		
		// well, you are out of luck
		return null;
	}
}
