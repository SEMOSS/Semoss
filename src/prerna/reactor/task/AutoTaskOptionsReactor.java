package prerna.reactor.task;

import java.util.List;
import java.util.Map;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.insight.InsightUtility;

public class AutoTaskOptionsReactor extends TaskBuilderReactor {

	private static final String IGNORE_PANEL_FILTERS = "ignoreFilters";

	public AutoTaskOptionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.LAYOUT_KEY.getKey(), IGNORE_PANEL_FILTERS};
	}

	@Override
	protected void buildTask() {
		organizeKeys();
		String panelId = this.keyValue.get(this.keysToGet[0]);
		String layout = this.keyValue.get(this.keysToGet[1]);
		if (layout == null) {
			// assume default value is grid
			layout = "GRID";
		}
		
		if (this.task instanceof BasicIteratorTask) {
			SelectQueryStruct qs = ((BasicIteratorTask) this.task).getQueryStruct();
			Map<String, Object> optMap = this.task.getFormatter().getOptionsMap();
			TaskOptions tOptions = AutoTaskOptionsHelper.getAutoOptions((BasicIteratorTask) this.task, qs, panelId, layout, optMap);
			if(tOptions != null) {
				this.task.setTaskOptions(tOptions);
				// if we use task options on a panel
				// we automatically set the panel view to be visualization
				InsightUtility.setPanelForVisualization(this.insight, panelId);
				
				// we will store this as the last run for this panel
				// and start to merge in the panel filters that were applied
				// determine if we are ignoring panel filters
				boolean ignorePanelFilters = ignorePanelFilters();
				if(!ignorePanelFilters) {
					this.insight.setFinalViewOptions(panelId, qs, task.getTaskOptions(), task.getFormatter());
					qs.addPanel(this.insight.getInsightPanel(panelId));
				}
			}
		}
	}
	
	private boolean ignorePanelFilters() {
		GenRowStruct grs = this.store.getNoun(IGNORE_PANEL_FILTERS);
		if(grs != null && !grs.isEmpty()) {
			return Boolean.parseBoolean(grs.get(0) + "");
			
		}
		
		List<Object> booleanInputs = curRow.getValuesOfType(PixelDataType.BOOLEAN);
		if(booleanInputs != null && !booleanInputs.isEmpty()) {
			return (boolean) booleanInputs.get(0);
		}
		
		return false;
	}
	
}
