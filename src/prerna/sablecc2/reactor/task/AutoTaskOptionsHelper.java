package prerna.sablecc2.reactor.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.task.options.TaskOptions;

public class AutoTaskOptionsHelper {

	public static TaskOptions getAutoOptions(SelectQueryStruct qs, String panelId, String layout) {
		return getAutoOptions(qs, panelId, layout, null);
	}

	public static TaskOptions getAutoOptions(SelectQueryStruct qs, String panelId, String layout, Map<String, Object> optMap) {
		Map<String, Object> optionsMap = null;
		String findlayout = layout.toUpperCase();
		if ("AREA".equals(findlayout)) {
			optionsMap = generateMultiValueTaskOptions(qs, panelId, "Area");
		} else if ("BOXWHISKER".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(qs, panelId, "BoxWhisker");
		} else if ("BUBBLE".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(qs, panelId, "Bubble");
		} else if ("BULLET".equals(findlayout)) {
			optionsMap = generateBulletTaskOptions(qs, panelId);
		}
		// TODO CHICLET
		// TODO Chloropleth
		else if ("COLUMN".equals(findlayout)) {
			optionsMap = generateMultiValueTaskOptions(qs, panelId, "Column");
		} else if ("CLOUD".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(qs, panelId, "Cloud");
		} else if ("CLUSTER".equals(findlayout)) {
			optionsMap = generateClusterTaskOptions(qs, panelId);
		} else if ("DENDROGRAM".equals(findlayout)) {
			optionsMap = generateDendrogramTaskOptions(qs, panelId);
		} else if ("FUNNEL".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(qs, panelId, "Funnel");
		}
		// TODO GANTT
		else if ("GAUGE".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(qs, panelId, "Gauge");
		} else if ("GRAPH".equals(findlayout)) {
			optionsMap = generateGraphTaskOptions(qs, optMap, panelId, "Graph");
		} else if ("GRAPHGL".equals(findlayout)) {
			optionsMap = generateGraphTaskOptions(qs, optMap, panelId, "GraphGL");
		} else if ("GRID".equals(findlayout)) {
			optionsMap = generateGridTaskOptions(qs, panelId);
		} else if ("HEATMAP".equals(findlayout)) {
			optionsMap = generateHeatMapTaskOptions(qs, panelId);
		} else if ("LINE".equals(findlayout)) {
			optionsMap = generateMultiValueTaskOptions(qs, panelId, "Line");
		}
		// TODO MAP
		else if ("PACK".equals(findlayout)) {
			optionsMap = generateMultiGroupTaskOptions(qs, panelId, "Pack");
		} else if ("PARALLELCOORDINATES".equals(findlayout)) {
			optionsMap = generateMultiDimensionTaskOptions(qs, panelId, "ParallelCoordinates");
		} else if ("PIE".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(qs, panelId, "Pie");
		} else if ("POLAR".equals(findlayout)) {
			optionsMap = generateMultiValueTaskOptions(qs, panelId, "Polar");
		} else if ("RADAR".equals(findlayout)) {
			optionsMap = generateMultiValueTaskOptions(qs, panelId, "Radar");
		} else if ("RADIAL".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(qs, panelId, "Radial");
		} else if ("SCATTER".equals(findlayout)) {
			optionsMap = generateScatterTaskOptions(qs, panelId);
		} else if ("SANKEY".equals(findlayout)) {
			optionsMap = generateSankeyTaskOptions(qs, panelId);
		} else if ("SCATTERPLOTMATRIX".equals(findlayout)) {
			optionsMap = generateMultiDimensionTaskOptions(qs, panelId, "ScatterplotMatrix");
		} else if ("SCATTER3D".equals(findlayout)) {
			optionsMap = generateScatter3DTaskOptions(qs, panelId);
		} else if ("SINGLEAXISCLUSTER".equals(findlayout)) {
			optionsMap = generateSingleAxisClusterTaskOptions(qs, panelId);
		} else if ("SUNBURST".equals(findlayout)) {
			optionsMap = generateMultiGroupTaskOptions(qs, panelId, "Sunburst");
		} else if ("TREEMAP".equals(findlayout)) {
			optionsMap = generateTreeMapTaskOptions(qs, panelId);
		} else if ("VIVAGRAP".equals(findlayout)) {
			optionsMap = generateGraphTaskOptions(qs, optMap, panelId, "VivaGraph");
		} else if ("WATERFALL".equals(findlayout)) {
			optionsMap = generateWaterfallTaskOptions(qs, panelId);
		}

		if(optionsMap == null) {
			return null;
		}
		return new TaskOptions(optionsMap);
	}


	public static Map<String, Object> generateWaterfallTaskOptions(SelectQueryStruct qs, String panelId) {
		List<IQuerySelector> selectors = qs.getSelectors();
		int size = selectors.size();
		// grab inputs
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (size >= 1) {
			labelMap.put("label", new String[] { selectors.get(0).getAlias() });
		}
		if (size >= 2) {
			labelMap.put("start", new String[] { selectors.get(1).getAlias() });
		}
		if (size >= 3) {
			labelMap.put("end", new String[] { selectors.get(2).getAlias() });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "Waterfall");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateTreeMapTaskOptions(SelectQueryStruct qs, String panelId) {
		List<IQuerySelector> selectors = qs.getSelectors();
		int size = selectors.size();
		// grab inputs
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (size >= 1) {
			labelMap.put("series", new String[] { selectors.get(0).getAlias() });
		}
		if (size >= 2) {
			labelMap.put("label", new String[] { selectors.get(1).getAlias() });
		}
		if (size >= 3) {
			labelMap.put("size", new String[] { selectors.get(2).getAlias() });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "TreeMap");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateSingleAxisClusterTaskOptions(SelectQueryStruct qs, String panelId) {
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
			labelMap.put("size", new String[] { selectors.get(2).getAlias() });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "SingleAxisCluster");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateSankeyTaskOptions(SelectQueryStruct qs, String panelId) {
		List<IQuerySelector> selectors = qs.getSelectors();
		int size = selectors.size();
		String[] labelArr = new String[size - 1];
		String[] valueArr = new String[1];
		for (int i = 0; i < size; i++) {
			if (i < size - 1) {
				labelArr[i] = selectors.get(i).getAlias();
			} else {
				valueArr[0] = selectors.get(i).getAlias();
			}
		}

		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		labelMap.put("label", labelArr);
		labelMap.put("value", valueArr);
		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "Sankey");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateMultiDimensionTaskOptions(SelectQueryStruct qs, String panelId, String layout) {
		List<IQuerySelector> selectors = qs.getSelectors();
		int size = selectors.size();
		String[] aliasArr = new String[size];
		for (int i = 0; i < size; i++) {
			aliasArr[i] = selectors.get(i).getAlias();
		}

		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		labelMap.put("dimension", aliasArr);
		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", layout);

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateMultiGroupTaskOptions(SelectQueryStruct qs, String panelId, String layout) {
		List<IQuerySelector> selectors = qs.getSelectors();
		int size = selectors.size();
		String[] groupArr = new String[size - 1];
		String[] valueArr = new String[1];
		for (int i = 0; i < size; i++) {
			if (i < size - 1) {
				groupArr[i] = selectors.get(i).getAlias();
			} else {
				valueArr[0] = selectors.get(i).getAlias();
			}
		}

		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		labelMap.put("group", groupArr);
		labelMap.put("value", valueArr);
		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", layout);

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateHeatMapTaskOptions(SelectQueryStruct qs, String panelId) {
		List<IQuerySelector> selectors = qs.getSelectors();
		int size = selectors.size();
		// grab inputs
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (size >= 1) {
			labelMap.put("x", new String[] { selectors.get(0).getAlias() });
		}
		if (size >= 2) {
			labelMap.put("y", new String[] { selectors.get(1).getAlias() });
		}
		if (size >= 3) {
			labelMap.put("heat", new String[] { selectors.get(2).getAlias() });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "HeatMap");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateGraphTaskOptions(SelectQueryStruct qs, Map<String, Object> optMap, String panelId, String layout) {
		if(optMap == null) {
			return null;
		}
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (optMap.containsKey("connections")) {
			String connections = (String) optMap.get("connections");
			String[] paths = connections.split(";");
			String[] startNodes = new String[paths.length];
			String[] endNodes = new String[paths.length];
			for (int i = 0; i < paths.length; i++) {
				String path = paths[i];
				if (path.contains(".")) {
					String[] pathVertex = path.split("\\.");
					startNodes[i] = pathVertex[0];
					endNodes[i] = pathVertex[1];
				}
			}
			labelMap.put("start", startNodes);
			labelMap.put("end", endNodes);
		}
		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", layout);

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateDendrogramTaskOptions(SelectQueryStruct qs, String panelId) {
		List<IQuerySelector> selectors = qs.getSelectors();
		int size = selectors.size();
		String[] aliasArr = new String[size];
		for (int i = 0; i < size; i++) {
			aliasArr[i] = selectors.get(i).getAlias();
		}

		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		labelMap.put("dimension", aliasArr);
		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "Dendrogram");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateClusterTaskOptions(SelectQueryStruct qs, String panelId) {
		List<IQuerySelector> selectors = qs.getSelectors();
		int size = selectors.size();
		// grab inputs
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (size >= 1) {
			labelMap.put("cluster", new String[] { selectors.get(0).getAlias() });
		}
		if (size >= 2) {
			labelMap.put("label", new String[] { selectors.get(1).getAlias() });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "Cluster");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateBulletTaskOptions(SelectQueryStruct qs, String panelId) {
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
		if (size >= 3) {
			labelMap.put("targetValue", new String[] { selectors.get(2).getAlias() });
		}
		if (size >= 4) {
			labelMap.put("badMarker", new String[] { selectors.get(3).getAlias() });
		}
		if (size >= 5) {
			labelMap.put("satisfactoryMarker", new String[] { selectors.get(4).getAlias() });
		}
		if (size >= 6) {
			labelMap.put("excellentMarker", new String[] { selectors.get(5).getAlias() });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "Bullet");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateGridTaskOptions(SelectQueryStruct qs, String panelId) {
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
	}

	public static Map<String, Object> generateSingleValueTaskOptions(SelectQueryStruct qs, String panelId, String layout) {
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
		panelMap.put("layout", layout);

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateScatterTaskOptions(SelectQueryStruct qs, String panelId) {
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
			labelMap.put("y", new String[] { selectors.get(2).getAlias() });
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

	public static Map<String, Object> generateScatter3DTaskOptions(SelectQueryStruct qs, String panelId) {
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
			labelMap.put("y", new String[] { selectors.get(2).getAlias() });
		}
		if (size >= 4) {
			labelMap.put("z", new String[] { selectors.get(3).getAlias() });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "Scatter3d");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateMultiValueTaskOptions(SelectQueryStruct qs, String panelId, String layout) {
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
		panelMap.put("layout", layout);

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

}
