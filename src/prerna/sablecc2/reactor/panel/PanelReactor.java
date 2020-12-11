package prerna.sablecc2.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.EmbeddedRoutineReactor;
import prerna.sablecc2.reactor.EmbeddedScriptReactor;
import prerna.sablecc2.reactor.GenericReactor;

public class PanelReactor extends AbstractReactor {
	
	public PanelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// first input is the name of the panel
		String panelId = this.keyValue.get(this.keysToGet[0]);
		InsightPanel insightPanel = this.insight.getInsightPanel(panelId);
		if(insightPanel == null) {
			throw new NullPointerException("Panel Id " + panelId + " does not exist");
		}
		NounMetadata noun = new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL);
		return noun;
	}
	
	@Override
	public void mergeUp() {
		if(parentReactor != null) {
	    	if(parentReactor instanceof EmbeddedScriptReactor || parentReactor instanceof EmbeddedRoutineReactor
	    			|| parentReactor instanceof GenericReactor) {
	    		parentReactor.getCurRow().add(execute());
	    	} else {
	    		GenRowStruct parentInput = parentReactor.getNounStore().makeNoun(PixelDataType.PANEL.getKey());
	    		parentInput.add(execute());
	    	}
		}
	}
}
