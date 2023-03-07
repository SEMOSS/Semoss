package prerna.sablecc2.reactor.task;

import java.util.Map;
import java.util.Set;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.insight.InsightUtility;

public class FormatReactor extends TaskBuilderReactor {
	
	public FormatReactor() {
		this.keysToGet = new String[]{"type", ReactorKeysEnum.OPTIONS.getKey()};
	}
	
	@Override
	protected void buildTask() {
		GenRowStruct type = this.store.getNoun("type");
		if(type != null && !type.isEmpty()) {
			String formatType = type.get(0).toString();
			task.setFormat(formatType);
		}
		GenRowStruct options = this.store.getNoun(keysToGet[1]);
		if(options != null && !options.isEmpty()) {
			Map<String, Object> optionValues = (Map<String, Object>) options.get(0);
			// do we have custom colors in the insight and not overriding those in the reactor?
			task.setFormatOptions(optionValues);
		} else {
			// do we have custom colors on the insight that we should add?
			
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
