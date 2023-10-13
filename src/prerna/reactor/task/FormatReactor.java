package prerna.reactor.task;

import java.awt.Color;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.Constants;
import prerna.util.insight.InsightUtility;

public class FormatReactor extends TaskBuilderReactor {
	
	public FormatReactor() {
		this.keysToGet = new String[]{"type", ReactorKeysEnum.OPTIONS.getKey()};
	}
	
	@Override
	protected void buildTask() {
		GenRowStruct type = this.store.getNoun("type");
		String formatType = null;
		if(type != null && !type.isEmpty()) {
			formatType = type.get(0).toString();
			task.setFormat(formatType);
		}
		Map<String, Object> optionValues = null;

		GenRowStruct options = this.store.getNoun(keysToGet[1]);
		if(options != null && !options.isEmpty()) {
			optionValues = (Map<String, Object>) options.get(0);
		}
		optionValues = applyDefaultOptions(optionValues);
		
		// set the option values into the task
		if(optionValues != null && !optionValues.isEmpty()) {
			task.setFormatOptions(optionValues);
		}
		
		if(this.task.getTaskOptions() != null) {
			Set<String> panelIds = this.task.getTaskOptions().getPanelIds();
			for(String panelId : panelIds) {
				// we will store this as the last run for this panel
				// and start to merge in the panel filters that were applied
				if(task instanceof BasicIteratorTask) {
					SelectQueryStruct qs = ((BasicIteratorTask) task).getQueryStruct();
					this.insight.setFinalViewOptions(panelId, qs, task.getTaskOptions(), task.getFormatter());
					qs.addPanel(this.insight.getInsightPanel(panelId));
				}
				
				// and set panel for visualization
				InsightUtility.setPanelForVisualization(this.insight, panelId);
			}
		}
	}
	
	/**
	 * Merge insight formats into the specific formats applied into the task
	 * @param optionValues
	 * @return
	 */
	private Map<String, Object> applyDefaultOptions(Map<String, Object> optionValues) {
		Map<String, Object> defaultOptions = new HashMap<>();
		// do we have custom colors on the insight that we should add?
		// check if the user has defined their own color scheme in the insight
		if (this.insight.getVarStore().get(Constants.GRAPH_COLORS) != null) {
			Map<String, Color> colorsMap = (Map<String, Color>) this.insight.getVarStore().get("GRAPH_COLORS").getValue();
			defaultOptions.put("colors", colorsMap);
		}
		
		if(optionValues == null || optionValues.isEmpty()) {
			return defaultOptions;
		}
		
		for(String key : defaultOptions.keySet()) {
			optionValues.putIfAbsent(key, defaultOptions.get(key));
		}
		return optionValues;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("type")) {
			return "The format type (e.g., a visualization can be the \"table\" or \"graph\" type)";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
