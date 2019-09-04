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

		Map<String, Object> optionsMap = null;
		String findlayout = layout.toUpperCase();
		if ("GRID".equals(findlayout)) {
			optionsMap = generateGridTaskOptions(panelId);
		} else if ("COLUMN".equals(findlayout)) {
			optionsMap = generateColumnTaskOptions(panelId);
		} else if ("LINE".equals(findlayout)) {
			optionsMap = generateLineTaskOptions(panelId);
		} else if ("SCATTER".equals(findlayout)) {
			optionsMap = generateScatterTaskOptions(panelId);
		} else if ("PIE".equals(findlayout)) {
			optionsMap = generatePieTaskOptions(panelId);
		} else if ("AREA".equals(findlayout)) {
			optionsMap = generateAreaTaskOptions(panelId);
		}

		if (optionsMap != null) {
			this.task.setTaskOptions(new TaskOptions(optionsMap));
		}
	}

	private Map<String, Object> generateAreaTaskOptions(String panelId) {
		if (this.task instanceof BasicIteratorTask) {
			SelectQueryStruct qs = ((BasicIteratorTask) this.task).getQueryStruct();
			List<IQuerySelector> selectors = qs.getSelectors();
			int size = selectors.size();
			// grab inputs
			String[] labelArray = new String[1];
			String[] valueArray = new String[size - 1];
			for (int i = 0; i < size; i++) {
				if (i == 0) {
					labelArray[i] = selectors.get(i).getAlias();
				} else {
					valueArray[i - 1] = selectors.get(i).getAlias();
				}
			}

			Map<String, String[]> labelMap = new HashMap<String, String[]>();
			labelMap.put("label", labelArray);
			labelMap.put("value", valueArray);
			Map<String, Object> panelMap = new HashMap<String, Object>();
			panelMap.put("alignment", labelMap);
			panelMap.put("layout", "Area");

			Map<String, Object> options = new HashMap<String, Object>();
			options.put(panelId, panelMap);
			return options;
		}
		return null;
	}

	private Map<String, Object> generateGridTaskOptions(String panelId) {
		if (this.task instanceof BasicIteratorTask) {
			SelectQueryStruct qs = ((BasicIteratorTask) this.task).getQueryStruct();
			List<IQuerySelector> selectors = qs.getSelectors();
			int size = selectors.size();
			String[] aliasArr = new String[size];
			for (int i = 0; i < size; i++) {
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

	private Map<String, Object> generatePieTaskOptions(String panelId) {
		if (this.task instanceof BasicIteratorTask) {
			SelectQueryStruct qs = ((BasicIteratorTask) this.task).getQueryStruct();
			List<IQuerySelector> selectors = qs.getSelectors();
			int size = selectors.size();
			// grab inputs
			Map<String, String[]> labelMap = new HashMap<String, String[]>();
			if (size >= 1) {
				labelMap.put("label", new String[] { selectors.get(0).getAlias() });
			}
			if (size >= 2) {
				labelMap.put("value", new String[] { selectors.get(1).getAlias() });
			}

			Map<String, Object> panelMap = new HashMap<String, Object>();
			panelMap.put("alignment", labelMap);
			panelMap.put("layout", "Pie");

			Map<String, Object> options = new HashMap<String, Object>();
			options.put(panelId, panelMap);
			return options;
		}
		return null;
	}

	private Map<String, Object> generateScatterTaskOptions(String panelId) {
		if (this.task instanceof BasicIteratorTask) {
			SelectQueryStruct qs = ((BasicIteratorTask) this.task).getQueryStruct();
			List<IQuerySelector> selectors = qs.getSelectors();
			int size = selectors.size();
			// grab inputs
			Map<String, String[]> labelMap = new HashMap<String, String[]>();
			if (size >= 1) {
				labelMap.put("label", new String[] { selectors.get(0).getAlias() });
			}
			if (size >= 2) {
				labelMap.put("x", new String[] { selectors.get(1).getAlias() });
			}
			if (size >= 3) {
				labelMap.put("x", new String[] { selectors.get(2).getAlias() });
			}
			if (size >= 4) {
				labelMap.put("z", new String[] { selectors.get(3).getAlias() });
			}
			if (size >= 5) {
				labelMap.put("series", new String[] { selectors.get(4).getAlias() });
			}

			Map<String, Object> panelMap = new HashMap<String, Object>();
			panelMap.put("alignment", labelMap);
			panelMap.put("layout", "Scatter");

			Map<String, Object> options = new HashMap<String, Object>();
			options.put(panelId, panelMap);
			return options;
		}
		return null;
	}

	private Map<String, Object> generateLineTaskOptions(String panelId) {
		if (this.task instanceof BasicIteratorTask) {
			SelectQueryStruct qs = ((BasicIteratorTask) this.task).getQueryStruct();
			List<IQuerySelector> selectors = qs.getSelectors();
			int size = selectors.size();
			// grab inputs
			String[] labelArray = new String[1];
			String[] valueArray = new String[size - 1];
			for (int i = 0; i < size; i++) {
				if (i == 0) {
					labelArray[i] = selectors.get(i).getAlias();
				} else {
					valueArray[i - 1] = selectors.get(i).getAlias();
				}
			}

			Map<String, String[]> labelMap = new HashMap<String, String[]>();
			labelMap.put("label", labelArray);
			labelMap.put("value", valueArray);
			Map<String, Object> panelMap = new HashMap<String, Object>();
			panelMap.put("alignment", labelMap);
			panelMap.put("layout", "Line");

			Map<String, Object> options = new HashMap<String, Object>();
			options.put(panelId, panelMap);
			return options;
		}
		return null;
	}

	private Map<String, Object> generateColumnTaskOptions(String panelId) {
		if (this.task instanceof BasicIteratorTask) {
			SelectQueryStruct qs = ((BasicIteratorTask) this.task).getQueryStruct();
			List<IQuerySelector> selectors = qs.getSelectors();
			int size = selectors.size();
			// label array
			String[] labelArray = new String[1];
			String[] valueArray = new String[size - 1];
			for (int i = 0; i < size; i++) {
				if (i == 0) {
					labelArray[i] = selectors.get(i).getAlias();
				} else {
					valueArray[i - 1] = selectors.get(i).getAlias();
				}
			}

			Map<String, String[]> labelMap = new HashMap<String, String[]>();
			labelMap.put("label", labelArray);
			labelMap.put("value", valueArray);
			Map<String, Object> panelMap = new HashMap<String, Object>();
			panelMap.put("alignment", labelMap);
			panelMap.put("layout", "Column");

			Map<String, Object> options = new HashMap<String, Object>();
			options.put(panelId, panelMap);
			return options;
		}

		return null;
	}
}
