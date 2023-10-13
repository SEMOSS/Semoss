package prerna.reactor.task;

import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.insight.InsightUtility;

public class TaskOptionsReactor extends TaskBuilderReactor {

	private static final String IGNORE_PANEL_FILTERS = "ignoreFilters";
	
	public TaskOptionsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.OPTIONS.getKey(), IGNORE_PANEL_FILTERS};
	}

	@Override
	protected void buildTask() {
		organizeKeys();
		List<Object> mapOptions = this.curRow.getValuesOfType(PixelDataType.MAP);
		if(mapOptions == null || mapOptions.size() == 0) {
			// if it is null, i guess we just clear the map values
			this.task.setTaskOptions(null);
		} else {
			this.task.setTaskOptions(new TaskOptions((Map<String, Object>) mapOptions.get(0)));
			
			// if we use task options on a panel
			// we automatically set the panel view to be visualization
			Set<String> panelIds = this.task.getTaskOptions().getPanelIds();
			for(String panelId : panelIds) {
				// we will store this as the last run for this panel
				// and start to merge in the panel filters that were applied
				if(task instanceof BasicIteratorTask) {
					SelectQueryStruct qs = ((BasicIteratorTask) task).getQueryStruct();
					this.insight.setFinalViewOptions(panelId, qs, task.getTaskOptions(), task.getFormatter());
					if(!ignorePanelFilters()) {
						qs.addPanel(this.insight.getInsightPanel(panelId));
					}
				}
				
				// and set panel for visualization
				InsightUtility.setPanelForVisualization(this.insight, panelId);
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
