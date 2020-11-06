package prerna.sablecc2.reactor.frame.filter;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public abstract class AbstractFilterReactor extends AbstractFrameReactor {

	protected String DYNAMIC_KEY = "dynamic";

	/**
	 * Get the filters passed into the reactor
	 * 
	 * @return
	 */
	protected GenRowFilters getFilters() {
		// generate a grf with the wanted filters
		GenRowFilters grf = new GenRowFilters();
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			IQueryFilter nextFilter = (IQueryFilter) this.curRow.get(i);
			if (nextFilter != null) {
				grf.addFilters(nextFilter);
			}
		}
		return grf;
	}

	protected InsightPanel getInsightPanel() {
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

		return null;
	}

}