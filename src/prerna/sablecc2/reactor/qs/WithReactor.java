package prerna.sablecc2.reactor.qs;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.EmbeddedRoutineReactor;
import prerna.sablecc2.reactor.EmbeddedScriptReactor;
import prerna.sablecc2.reactor.GenericReactor;

public class WithReactor extends AbstractQueryStructReactor {
	
	public WithReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		InsightPanel panel = getPanel();
		GenRowFilters panelFilters = panel.getPanelFilters();
		qs.mergeImplicitFilters(panelFilters.copy());
		List<IQuerySort> orderBys = panel.getPanelOrderBys();
		((SelectQueryStruct) qs).mergeOrderBy(orderBys);
		return qs;
	}
	
	/**
	 * Get the panel filter grs
	 * @return
	 */
	private InsightPanel getPanel() {
		InsightPanel panel = null;

		// see if panel was passed via generic reactor
		GenRowStruct genericGrs = this.store.getNoun(keysToGet[0]);
		if(genericGrs != null && !genericGrs.isEmpty()) {
			String panelId = genericGrs.get(0).toString();
			panel = this.insight.getInsightPanel(panelId);
		}

		if(panel == null) {
			// if not, see if it was passed in the grs
			List<Object> panelNouns = this.curRow.getValuesOfType(PixelDataType.PANEL);
			if(panelNouns != null && !panelNouns.isEmpty()) {
				panel = (InsightPanel) panelNouns.get(0);
			}
		}

		if(panel == null) {
			throw new IllegalArgumentException("Invalid panel id passed into With reactor");
		}

		return panel;
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
			NounMetadata data = new NounMetadata(this.qs, PixelDataType.QUERY_STRUCT);
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
