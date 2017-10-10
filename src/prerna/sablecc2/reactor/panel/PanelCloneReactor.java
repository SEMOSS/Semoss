package prerna.sablecc2.reactor.panel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class PanelCloneReactor extends AbstractInsightPanelReactor {

	private static final String CLONE_PANEL_KEY = "cloneId";

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel existingPanel = getInsightPanel();

		// get the new panel id
		String newId = getClonePanelId();
		// make the new panel
		InsightPanel newClonePanel = new InsightPanel(newId);
		newClonePanel.clone(existingPanel);
		
		// remember to add the new panel into the insight
		this.insight.addNewInsightPanel(newClonePanel);
		
		Map<String, InsightPanel> cloneMap = new HashMap<String, InsightPanel>();
		cloneMap.put("original", existingPanel);
		cloneMap.put("clone", newClonePanel);
		// return the new panel
		return new NounMetadata(cloneMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PANEL_CLONE);
	}
	
	private String getClonePanelId() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(CLONE_PANEL_KEY);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return genericReactorGrs.get(0).toString();
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		// try if it is a string, int, or double
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
		if(strNouns != null && !strNouns.isEmpty()) {
			return strNouns.get(0).getValue().toString();
		}
		strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_INT);
		if(strNouns != null && !strNouns.isEmpty()) {
			return strNouns.get(0).getValue().toString();
		}
		strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_DECIMAL);
		if(strNouns != null && !strNouns.isEmpty()) {
			return strNouns.get(0).getValue().toString();
		}
		
		// well, you are out of luck
		return null;
	}
}