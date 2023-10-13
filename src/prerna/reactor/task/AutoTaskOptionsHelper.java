package prerna.reactor.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.options.TaskOptions;

public class AutoTaskOptionsHelper {

	public static TaskOptions getAutoOptions(SelectQueryStruct qs, String panelId, String layout) {
		return getAutoOptions(getSelectorsFromQs(qs), panelId, layout, null);
	}

	public static TaskOptions getAutoOptions(SelectQueryStruct qs, String panelId, String layout, Map<String, Object> optMap) {
		return getAutoOptions(getSelectorsFromQs(qs), panelId, layout, optMap);
	}
	
	public static TaskOptions getAutoOptions(BasicIteratorTask task, SelectQueryStruct qs, String panelId, String layout) throws Exception {
		return getAutoOptions(task, qs, panelId, layout, null);
	}

	public static TaskOptions getAutoOptions(BasicIteratorTask task, SelectQueryStruct qs, String panelId, String layout, Map<String, Object> optMap) {
		List<String> selectors = null;
		if(qs instanceof HardSelectQueryStruct && task != null) {
			try {
				selectors = Arrays.asList(task.getIterator().getHeaders());
			} catch (Exception e) {
				throw new IllegalArgumentException("Error in generating iterator", e);
			}
		} else {
			selectors = getSelectorsFromQs(qs);
		}
		return getAutoOptions(selectors, panelId, layout, optMap);
	}

	public static TaskOptions getAutoOptions(List<String> selectors, String panelId, String layout) {
		return getAutoOptions(selectors, panelId, layout, null);
	}

	public static TaskOptions getAutoOptions(List<String> selectors, String panelId, String layout, Map<String, Object> optMap) {
		Map<String, Object> optionsMap = null;
		String findlayout = layout.toUpperCase();
		if ("AREA".equals(findlayout)) {
			optionsMap = generateMultiValueTaskOptions(selectors, panelId, "Area");
		} else if ("BOXWHISKER".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(selectors, panelId, "BoxWhisker");
		} else if ("BUBBLE".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(selectors, panelId, "Bubble");
		} else if ("BULLET".equals(findlayout)) {
			optionsMap = generateBulletTaskOptions(selectors, panelId);
		}
		// TODO CHICLET
		// TODO Chloropleth
		else if ("COLUMN".equals(findlayout)) {
			optionsMap = generateMultiValueTaskOptions(selectors, panelId, "Column");
		} else if ("CLOUD".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(selectors, panelId, "Cloud");
		} else if ("CLUSTER".equals(findlayout)) {
			optionsMap = generateClusterTaskOptions(selectors, panelId);
		} else if ("DENDROGRAM".equals(findlayout)) {
			optionsMap = generateDendrogramTaskOptions(selectors, panelId);
		} else if ("FUNNEL".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(selectors, panelId, "Funnel");
		}
		// TODO GANTT
		else if ("GAUGE".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(selectors, panelId, "Gauge");
		} else if ("GRAPH".equals(findlayout)) {
			optionsMap = generateGraphTaskOptions(optMap, panelId, "Graph");
		} else if ("GRAPHGL".equals(findlayout)) {
			optionsMap = generateGraphTaskOptions(optMap, panelId, "GraphGL");
		} else if ("GRID".equals(findlayout)) {
			optionsMap = generateGridTaskOptions(selectors, panelId);
		} else if ("HEATMAP".equals(findlayout)) {
			optionsMap = generateHeatMapTaskOptions(selectors, panelId);
		} else if ("LINE".equals(findlayout)) {
			optionsMap = generateMultiValueTaskOptions(selectors, panelId, "Line");
		}
		// TODO MAP
		else if ("PACK".equals(findlayout)) {
			optionsMap = generateMultiGroupTaskOptions(selectors, panelId, "Pack");
		} else if ("PARALLELCOORDINATES".equals(findlayout)) {
			optionsMap = generateMultiDimensionTaskOptions(selectors, panelId, "ParallelCoordinates");
		} else if ("PIE".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(selectors, panelId, "Pie");
		} else if ("POLAR".equals(findlayout)) {
			optionsMap = generateMultiValueTaskOptions(selectors, panelId, "Polar");
		} else if ("RADAR".equals(findlayout)) {
			optionsMap = generateMultiValueTaskOptions(selectors, panelId, "Radar");
		} else if ("RADIAL".equals(findlayout)) {
			optionsMap = generateSingleValueTaskOptions(selectors, panelId, "Radial");
		} else if ("SCATTER".equals(findlayout)) {
			optionsMap = generateScatterTaskOptions(selectors, panelId);
		} else if ("SANKEY".equals(findlayout)) {
			optionsMap = generateSankeyTaskOptions(selectors, panelId);
		} else if ("SCATTERPLOTMATRIX".equals(findlayout)) {
			optionsMap = generateMultiDimensionTaskOptions(selectors, panelId, "ScatterplotMatrix");
		} else if ("SCATTER3D".equals(findlayout)) {
			optionsMap = generateScatter3DTaskOptions(selectors, panelId);
		} else if ("SINGLEAXISCLUSTER".equals(findlayout)) {
			optionsMap = generateSingleAxisClusterTaskOptions(selectors, panelId);
		} else if ("SUNBURST".equals(findlayout)) {
			optionsMap = generateMultiGroupTaskOptions(selectors, panelId, "Sunburst");
		} else if ("TREEMAP".equals(findlayout)) {
			optionsMap = generateTreeMapTaskOptions(selectors, panelId);
		} else if ("VIVAGRAP".equals(findlayout)) {
			optionsMap = generateGraphTaskOptions(optMap, panelId, "VivaGraph");
		} else if ("WATERFALL".equals(findlayout)) {
			optionsMap = generateWaterfallTaskOptions(selectors, panelId);
		}

		if(optionsMap == null) {
			return null;
		}
		return new TaskOptions(optionsMap);
	}

	public static Map<String, Object> generateWaterfallTaskOptions(SelectQueryStruct qs, String panelId) {
		return generateWaterfallTaskOptions(getSelectorsFromQs(qs), panelId);
	}

	public static Map<String, Object> generateTreeMapTaskOptions(SelectQueryStruct qs, String panelId) {
		return generateTreeMapTaskOptions(getSelectorsFromQs(qs), panelId);
	}

	public static Map<String, Object> generateSingleAxisClusterTaskOptions(SelectQueryStruct qs, String panelId) {
		return generateSingleAxisClusterTaskOptions(getSelectorsFromQs(qs), panelId);
	}

	public static Map<String, Object> generateSankeyTaskOptions(SelectQueryStruct qs, String panelId) {
		return generateSankeyTaskOptions(getSelectorsFromQs(qs), panelId);
	}

	public static Map<String, Object> generateMultiDimensionTaskOptions(SelectQueryStruct qs, String panelId, String layout) {
		return generateMultiDimensionTaskOptions(getSelectorsFromQs(qs), panelId, layout);
	}

	public static Map<String, Object> generateMultiGroupTaskOptions(SelectQueryStruct qs, String panelId, String layout) {
		return generateMultiGroupTaskOptions(getSelectorsFromQs(qs), panelId, layout);
	}

	public static Map<String, Object> generateHeatMapTaskOptions(SelectQueryStruct qs, String panelId) {
		return generateHeatMapTaskOptions(getSelectorsFromQs(qs), panelId);
	}

	public static Map<String, Object> generateGraphTaskOptions(Map<String, Object> optMap, String panelId, String layout) {
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
		return generateDendrogramTaskOptions(getSelectorsFromQs(qs), panelId);
	}

	public static Map<String, Object> generateClusterTaskOptions(SelectQueryStruct qs, String panelId) {
		return generateClusterTaskOptions(getSelectorsFromQs(qs), panelId);
	}

	public static Map<String, Object> generateBulletTaskOptions(SelectQueryStruct qs, String panelId) {
		return generateBulletTaskOptions(getSelectorsFromQs(qs), panelId);
	}

	public static Map<String, Object> generateGridTaskOptions(SelectQueryStruct qs, String panelId) {
		return generateGridTaskOptions(getSelectorsFromQs(qs), panelId);
	}

	public static Map<String, Object> generateSingleValueTaskOptions(SelectQueryStruct qs, String panelId, String layout) {
		return generateSingleValueTaskOptions(getSelectorsFromQs(qs), panelId, layout);
	}

	public static Map<String, Object> generateScatterTaskOptions(SelectQueryStruct qs, String panelId) {
		return generateScatterTaskOptions(getSelectorsFromQs(qs), panelId);
	}

	public static Map<String, Object> generateScatter3DTaskOptions(SelectQueryStruct qs, String panelId) {
		return generateScatter3DTaskOptions(getSelectorsFromQs(qs), panelId);
	}

	public static Map<String, Object> generateMultiValueTaskOptions(SelectQueryStruct qs, String panelId, String layout) {
		return generateMultiValueTaskOptions(getSelectorsFromQs(qs), panelId, layout);
	}

	/**
	 * Flush selectors from qs
	 * @param qs
	 * @return
	 */
	private static List<String> getSelectorsFromQs(SelectQueryStruct qs) {
		List<IQuerySelector> qsSelectors = qs.getSelectors();
		int size = qsSelectors.size();
		
		List<String> selectors = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			selectors.add(qsSelectors.get(i).getAlias());
		}
		
		return selectors;
	}

	public static Map<String, Object> generateWaterfallTaskOptions(List<String> selectors, String panelId) {
		int size = selectors.size();
		// grab inputs
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (size >= 1) {
			labelMap.put("label", new String[] { selectors.get(0) });
		}
		if (size >= 2) {
			labelMap.put("start", new String[] { selectors.get(1) });
		}
		if (size >= 3) {
			labelMap.put("end", new String[] { selectors.get(2) });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "Waterfall");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateTreeMapTaskOptions(List<String> selectors, String panelId) {
		int size = selectors.size();
		// grab inputs
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (size >= 1) {
			labelMap.put("series", new String[] { selectors.get(0) });
		}
		if (size >= 2) {
			labelMap.put("label", new String[] { selectors.get(1) });
		}
		if (size >= 3) {
			labelMap.put("size", new String[] { selectors.get(2) });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "TreeMap");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateSingleAxisClusterTaskOptions(List<String> selectors, String panelId) {
		int size = selectors.size();
		// grab inputs
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (size >= 1) {
			labelMap.put("label", new String[] { selectors.get(0) });
		}
		if (size >= 2) {
			labelMap.put("x", new String[] { selectors.get(1) });
		}
		if (size >= 3) {
			labelMap.put("size", new String[] { selectors.get(2) });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "SingleAxisCluster");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateSankeyTaskOptions(List<String> selectors, String panelId) {
		int size = selectors.size();
		String[] labelArr = new String[size - 1];
		String[] valueArr = new String[1];
		for (int i = 0; i < size; i++) {
			if (i < size - 1) {
				labelArr[i] = selectors.get(i);
			} else {
				valueArr[0] = selectors.get(i);
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

	public static Map<String, Object> generateMultiDimensionTaskOptions(List<String> selectors, String panelId, String layout) {
		int size = selectors.size();
		String[] aliasArr = new String[size];
		for (int i = 0; i < size; i++) {
			aliasArr[i] = selectors.get(i);
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

	public static Map<String, Object> generateMultiGroupTaskOptions(List<String> selectors, String panelId, String layout) {
		int size = selectors.size();
		String[] groupArr = new String[size - 1];
		String[] valueArr = new String[1];
		for (int i = 0; i < size; i++) {
			if (i < size - 1) {
				groupArr[i] = selectors.get(i);
			} else {
				valueArr[0] = selectors.get(i);
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

	public static Map<String, Object> generateHeatMapTaskOptions(List<String> selectors, String panelId) {
		int size = selectors.size();
		// grab inputs
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (size >= 1) {
			labelMap.put("x", new String[] { selectors.get(0) });
		}
		if (size >= 2) {
			labelMap.put("y", new String[] { selectors.get(1) });
		}
		if (size >= 3) {
			labelMap.put("heat", new String[] { selectors.get(2) });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "HeatMap");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateDendrogramTaskOptions(List<String> selectors, String panelId) {
		int size = selectors.size();
		String[] aliasArr = new String[size];
		for (int i = 0; i < size; i++) {
			aliasArr[i] = selectors.get(i);
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

	public static Map<String, Object> generateClusterTaskOptions(List<String> selectors, String panelId) {
		int size = selectors.size();
		// grab inputs
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (size >= 1) {
			labelMap.put("cluster", new String[] { selectors.get(0) });
		}
		if (size >= 2) {
			labelMap.put("label", new String[] { selectors.get(1) });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "Cluster");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateBulletTaskOptions(List<String> selectors, String panelId) {
		int size = selectors.size();
		// grab inputs
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (size >= 1) {
			labelMap.put("label", new String[] { selectors.get(0) });
		}
		if (size >= 2) {
			labelMap.put("value", new String[] { selectors.get(1) });
		}
		if (size >= 3) {
			labelMap.put("targetValue", new String[] { selectors.get(2) });
		}
		if (size >= 4) {
			labelMap.put("badMarker", new String[] { selectors.get(3) });
		}
		if (size >= 5) {
			labelMap.put("satisfactoryMarker", new String[] { selectors.get(4) });
		}
		if (size >= 6) {
			labelMap.put("excellentMarker", new String[] { selectors.get(5) });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "Bullet");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateGridTaskOptions(List<String> selectors, String panelId) {
		int size = selectors.size();
		String[] aliasArr = new String[size];
		for (int i = 0; i < size; i++) {
			aliasArr[i] = selectors.get(i);
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

	public static Map<String, Object> generateSingleValueTaskOptions(List<String> selectors, String panelId, String layout) {
		int size = selectors.size();
		// grab inputs
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (size >= 1) {
			labelMap.put("label", new String[] { selectors.get(0) });
		}
		if (size >= 2) {
			labelMap.put("value", new String[] { selectors.get(1) });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", layout);

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateScatterTaskOptions(List<String> selectors, String panelId) {
		int size = selectors.size();
		// grab inputs
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (size >= 1) {
			labelMap.put("label", new String[] { selectors.get(0) });
		}
		if (size >= 2) {
			labelMap.put("x", new String[] { selectors.get(1) });
		}
		if (size >= 3) {
			labelMap.put("y", new String[] { selectors.get(2) });
		}
		if (size >= 4) {
			labelMap.put("z", new String[] { selectors.get(3) });
		}
		if (size >= 5) {
			labelMap.put("series", new String[] { selectors.get(4) });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "Scatter");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateScatter3DTaskOptions(List<String> selectors, String panelId) {
		int size = selectors.size();
		// grab inputs
		Map<String, String[]> labelMap = new HashMap<String, String[]>();
		if (size >= 1) {
			labelMap.put("label", new String[] { selectors.get(0) });
		}
		if (size >= 2) {
			labelMap.put("x", new String[] { selectors.get(1) });
		}
		if (size >= 3) {
			labelMap.put("y", new String[] { selectors.get(2) });
		}
		if (size >= 4) {
			labelMap.put("z", new String[] { selectors.get(3) });
		}

		Map<String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put("alignment", labelMap);
		panelMap.put("layout", "Scatter3d");

		Map<String, Object> options = new HashMap<String, Object>();
		options.put(panelId, panelMap);
		return options;
	}

	public static Map<String, Object> generateMultiValueTaskOptions(List<String> selectors, String panelId, String layout) {
		int size = selectors.size();
		// label array
		String[] labelArray = new String[1];
		String[] valueArray = new String[size - 1];
		for (int i = 0; i < size; i++) {
			if (i == 0) {
				labelArray[i] = selectors.get(i);
			} else {
				valueArray[i - 1] = selectors.get(i);
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
