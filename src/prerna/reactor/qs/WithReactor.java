package prerna.reactor.qs;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.reactor.EmbeddedRoutineReactor;
import prerna.reactor.EmbeddedScriptReactor;
import prerna.reactor.GenericReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class WithReactor extends AbstractQueryStructReactor {
	
	public WithReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		InsightPanel panel = getPanel();
		this.qs.addPanel(panel);
		
		/*
		 * 
		 * THIS REACTOR IS NOT REQUIRED ANYMORE
		 * THIS IS AUTOMATICALLY ASSUMED WHEN A 
		 * TASK OPTIONS IS ADDED IN THE QUERY
		 * 
		 */
		
		return this.qs;
	}
	
	/**
	 * Get the panel filter grs
	 * @return
	 */
	private InsightPanel getPanel() {
		// passed in directly as panel
		GenRowStruct genericReactorGrs = this.store.getNoun(ReactorKeysEnum.PANEL.getKey());
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			NounMetadata noun = genericReactorGrs.getNoun(0);
			PixelDataType nounType = noun.getNounType();
			if(nounType == PixelDataType.PANEL) {
				return (InsightPanel) noun.getValue();
			} else if(nounType == PixelDataType.COLUMN || nounType == PixelDataType.CONST_STRING
					|| nounType == PixelDataType.CONST_INT) {
				String panelId = noun.getValue().toString();
				return this.insight.getInsightPanel(panelId);
			}
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
		
		throw new IllegalArgumentException("Invalid panel id passed into With reactor");
	}
	
	@Override
	public void mergeUp() {
		// merge this reactor into the parent reactor
		init();
//		createQueryStruct();
//		setAlias(qs.getSelectors(), selectorAlias, existingSelectors);
		if(parentReactor != null) {
			// this is only called lazy
			// have to init to set the qs
			// to them add to the parent
			NounMetadata data = new NounMetadata(createQueryStruct(), PixelDataType.QUERY_STRUCT);
	    	if(parentReactor instanceof EmbeddedScriptReactor || parentReactor instanceof EmbeddedRoutineReactor
	    			|| parentReactor instanceof GenericReactor) {
	    		parentReactor.getCurRow().add(data);
	    	} else {
	    		GenRowStruct parentQSInput = parentReactor.getNounStore().makeNoun(PixelDataType.QUERY_STRUCT.getKey());
				parentQSInput.add(data);
	    	}
		}
	}
}
