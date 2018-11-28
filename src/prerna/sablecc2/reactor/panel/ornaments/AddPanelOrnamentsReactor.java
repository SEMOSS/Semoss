package prerna.sablecc2.reactor.panel.ornaments;

import java.util.List;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.panel.AbstractInsightPanelReactor;

public class AddPanelOrnamentsReactor extends AbstractInsightPanelReactor {
	
	public AddPanelOrnamentsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.ORNAMENTS_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		// get the ornaments that come as a map
		Map<String, Object> ornaments = getOrnamentsMapInput();
		if(ornaments == null) {
			throw new IllegalArgumentException("Need to define the ornament input");
		}
		// merge the map options
		insightPanel.addOrnaments(ornaments);
		return new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL_ORNAMENT);
	}

	private Map<String, Object> getOrnamentsMapInput() {
		// see if it was passed directly in with the lower case key ornaments
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
