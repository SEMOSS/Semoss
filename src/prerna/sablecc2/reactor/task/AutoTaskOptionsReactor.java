package prerna.sablecc2.reactor.task;

import java.util.Map;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.insight.InsightUtility;

public class AutoTaskOptionsReactor extends TaskBuilderReactor {

	public AutoTaskOptionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.LAYOUT_KEY.getKey() };
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
			TaskOptions tOptions = AudoTaskOptionsHelper.getAutoOptions(qs, panelId, layout, optMap);
			if(tOptions != null) {
				this.task.setTaskOptions(tOptions);
				// if we use task options on a panel
				// we automatically set the panel view to be visualization
				InsightUtility.setPanelForVisualization(this.insight, panelId);
			}
		}
	}
}
