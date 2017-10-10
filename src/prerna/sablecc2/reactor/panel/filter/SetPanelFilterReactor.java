package prerna.sablecc2.reactor.panel.filter;

import java.util.Set;

import prerna.om.InsightPanel;
import prerna.query.querystruct.GenRowFilters;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.frame.filter.AbstractFilterReactor;

public class SetPanelFilterReactor extends AbstractFilterReactor {

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		GenRowFilters panelGrf = panel.getPanelFilters();
		
		// get the filters
		GenRowFilters newFilters = getFilters();
		if(newFilters.isEmpty()) {
			throw new IllegalArgumentException("No filter found to set to panel");
		}
		
		// remove the existing filters for the columns affected
		Set<String> allColsUsed = newFilters.getAllFilteredColumns();
		panelGrf.removeColumnFilters(allColsUsed);
		// add the new filters
		panelGrf.merge(newFilters);
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.PANEL_FILTER);
		return noun;
	}

}
