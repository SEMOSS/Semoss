package prerna.sablecc2.reactor.panel;

import java.util.List;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class AbstractInsightPanelReactor extends AbstractReactor {

//	protected static final String PANEL_KEY = "panel";
	protected static final String TRAVERSAL_KEY = ReactorKeysEnum.TRAVERSAL.getKey();

	protected InsightPanel getInsightPanel() {
		// passed in directly as panel
		GenRowStruct genericReactorGrs = this.store.getNoun(ReactorKeysEnum.PANEL.getKey());
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			NounMetadata noun = genericReactorGrs.getNoun(0);
			PixelDataType nounType = noun.getNounType();
			if(nounType == PixelDataType.PANEL) {
				return (InsightPanel) noun.getValue();
			} else if(nounType == PixelDataType.PANEL_CLONE_MAP) {
				Map<String, InsightPanel> cloneMap = (Map<String, InsightPanel>) noun.getValue();
				return cloneMap.get("clone");
			} else if(nounType == PixelDataType.COLUMN || nounType == PixelDataType.CONST_STRING) {
				String panelId = noun.getValue().toString();
				return this.insight.getInsightPanel(panelId);
			}
		}
		
		// look at all the ways the insight panel could be passed
		// look at store if it was passed in
		genericReactorGrs = this.store.getNoun(PixelDataType.PANEL.toString());
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (InsightPanel) genericReactorGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PixelDataType.PANEL);
		if(panelNouns != null && !panelNouns.isEmpty()) {
			return (InsightPanel) panelNouns.get(0).getValue();
		}
		
		// see if string or column passed in
		List<String> strInputs = this.curRow.getAllStrValues();
		if(strInputs != null && !strInputs.isEmpty()) {
			for(String panelId : strInputs) {
				InsightPanel panel = this.insight.getInsightPanel(panelId);
				if(panel != null) {
					return panel;
				}
			}
		}
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_INT);
		if(strNouns != null && !strNouns.isEmpty()) {
			return this.insight.getInsightPanel(strNouns.get(0).getValue().toString());
		}
		
		// see if a clone map was passed
		genericReactorGrs = this.store.getNoun(PixelDataType.PANEL_CLONE_MAP.toString());
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			NounMetadata noun = genericReactorGrs.getNoun(0);
			Map<String, InsightPanel> cloneMap = (Map<String, InsightPanel>) noun.getValue();
			return cloneMap.get("clone");
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		panelNouns = this.curRow.getNounsOfType(PixelDataType.PANEL_CLONE_MAP);
		if(panelNouns != null && !panelNouns.isEmpty()) {
			NounMetadata noun = genericReactorGrs.getNoun(0);
			Map<String, InsightPanel> cloneMap = (Map<String, InsightPanel>) noun.getValue();
			return cloneMap.get("clone");
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected String getTraversalLiteralInput() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(TRAVERSAL_KEY);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return genericReactorGrs.get(0).toString();
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
		if(strNouns != null && !strNouns.isEmpty()) {
			return strNouns.get(0).getValue().toString();
		}
		
		// well, you are out of luck
		return null;
	}
}
