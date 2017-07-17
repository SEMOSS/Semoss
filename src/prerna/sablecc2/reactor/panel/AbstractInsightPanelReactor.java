package prerna.sablecc2.reactor.panel;

import java.util.List;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class AbstractInsightPanelReactor extends AbstractReactor {

	protected static final String PANEL_KEY = "panel";
	protected static final String ORNAMENTS_KEY = "ornaments";
	protected static final String ORNAMENTS_TRAVERSAL_KEY = "key";
	protected static final String CLONE_PANEL_KEY = "cloneId";
	protected static final String PANEL_LABEL_KEY = "panelLabel";

	protected InsightPanel getInsightPanel() {
		// look at all the ways the insight panel could be passed
		// look at store if it was passed in
		GenRowStruct syncedPanelGrs = this.store.getNoun(PkslDataTypes.PANEL.toString());
		if(syncedPanelGrs != null && !syncedPanelGrs.isEmpty()) {
			return (InsightPanel) syncedPanelGrs.get(0);
		}
		
		// see if it was passed directly in with the lower case key panel
		GenRowStruct genericReactorGrs = this.store.getNoun(PkslDataTypes.PANEL.toString());
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (InsightPanel) genericReactorGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PkslDataTypes.PANEL);
		if(panelNouns != null && !panelNouns.isEmpty()) {
			return (InsightPanel) panelNouns.get(0).getValue();
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected Map<String, Object> getOrnamentsMapInput() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(ORNAMENTS_KEY);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (Map<String, Object>) genericReactorGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PkslDataTypes.MAP);
		if(panelNouns != null && !panelNouns.isEmpty()) {
			return (Map<String, Object>) panelNouns.get(0).getValue();
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected String getTraversalLiteralInput() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(ORNAMENTS_TRAVERSAL_KEY);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return genericReactorGrs.get(0).toString();
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PkslDataTypes.CONST_STRING);
		if(strNouns != null && !strNouns.isEmpty()) {
			return strNouns.get(0).getValue().toString();
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected String getClonePanelId() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(CLONE_PANEL_KEY);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return genericReactorGrs.get(0).toString();
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		// try if it is a string, int, or double
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PkslDataTypes.CONST_STRING);
		if(strNouns != null && !strNouns.isEmpty()) {
			return strNouns.get(0).getValue().toString();
		}
		strNouns = this.curRow.getNounsOfType(PkslDataTypes.CONST_INT);
		if(strNouns != null && !strNouns.isEmpty()) {
			return strNouns.get(0).getValue().toString();
		}
		strNouns = this.curRow.getNounsOfType(PkslDataTypes.CONST_DECIMAL);
		if(strNouns != null && !strNouns.isEmpty()) {
			return strNouns.get(0).getValue().toString();
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected String getPanelLabel() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(PANEL_LABEL_KEY);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return genericReactorGrs.get(0).toString();
		}

		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PkslDataTypes.CONST_STRING);
		if(strNouns != null && !strNouns.isEmpty()) {
			return strNouns.get(0).getValue().toString();
		}
		
		return "";
	}
	
}
