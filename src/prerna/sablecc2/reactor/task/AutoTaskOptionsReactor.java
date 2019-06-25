package prerna.sablecc2.reactor.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.options.TaskOptions;

public class AutoTaskOptionsReactor extends TaskBuilderReactor {
	
	public AutoTaskOptionsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.LAYOUT_KEY.getKey()};
	}

	@Override
	protected void buildTask() {
		organizeKeys();
		String panelId = this.keyValue.get(this.keysToGet[0]);
		String layout = this.keyValue.get(this.keysToGet[1]);
		if(layout == null) {
			// assume default value is grid
			layout = "GRID";
		}
		
		Map<String, Object> optionsMap = null;
		String findlayout = layout.toUpperCase();
		if("GRID".equals(findlayout)) {
			optionsMap = generateGridTaskOptions(panelId);
		}
		
		if(optionsMap != null) {
			this.task.setTaskOptions(new TaskOptions(optionsMap));
		}
	}
	
	private Map<String, Object> generateGridTaskOptions(String panelId) {
		if(this.task instanceof BasicIteratorTask) {
			SelectQueryStruct qs = ((BasicIteratorTask) this.task).getQueryStruct();
			List<IQuerySelector> selectors = qs.getSelectors();
			int size = selectors.size();
			String[] aliasArr = new String[size];
			for(int i = 0; i < size; i++) {
				aliasArr[i] = selectors.get(i).getAlias();
			}
			
			Map<String, String[]> labelMap = new HashMap<String, String[]>();
			labelMap.put("label", aliasArr);
			Map<String, Object> panelMap = new HashMap<String, Object>();
			panelMap.put("alignment", labelMap);
			panelMap.put("layout", "Grid");

			Map<String, Object> options = new HashMap<String, Object>();
			options.put(panelId, panelMap);
			return options;
		} else {
			// will account for other things here...
			
		}
		
		return null;
	}
}
