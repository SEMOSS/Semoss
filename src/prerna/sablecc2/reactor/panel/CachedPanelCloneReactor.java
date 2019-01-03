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

public class CachedPanelCloneReactor extends AbstractInsightPanelReactor {
	
	/**
	 * This code is the same as the Panel Clone Reactor but doesn't actually do the clone
	 * It is just to let the FE know that a clone was performed
	 * 
	 * It is only intended to be used to simplify the 
	 * cached insight recipe into a single call to get the panel
	 * state instead of multiple calls for each portion of the insight
	 * 
	 */
	
	public CachedPanelCloneReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.CLONE_PANEL_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel existingPanel = getInsightPanel();

		// get the new panel id
		String cloneId = getClonePanelId();
		// grab the clone panel
		InsightPanel clonePanel = this.insight.getInsightPanel(cloneId);
		
		Map<String, InsightPanel> cloneMap = new HashMap<String, InsightPanel>();
		cloneMap.put("original", existingPanel);
		cloneMap.put("clone", clonePanel);
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