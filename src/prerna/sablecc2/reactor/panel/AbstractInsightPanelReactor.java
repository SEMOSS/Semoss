package prerna.sablecc2.reactor.panel;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class AbstractInsightPanelReactor extends AbstractReactor {

//	protected static final String PANEL_KEY = "panel";
	protected static final String TRAVERSAL_KEY = ReactorKeysEnum.TRAVERSAL.getKey();

	protected InsightPanel getInsightPanel() {
		GenRowStruct genericReactorGrs = this.store.getNoun(ReactorKeysEnum.PANEL.getKey());
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (InsightPanel) genericReactorGrs.get(0);
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
