package prerna.sablecc2.translations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.om.Insight;
import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.AGeneric;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.AScalarRegTerm;
import prerna.sablecc2.node.POpInput;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;

public class OptimizeRecipeTranslation extends DepthFirstAdapter {

	// create logger
	private static final Logger LOGGER = LogManager.getLogger(OptimizeRecipeTranslation.class.getName());

	// set of reactors that send back task data to visualize
	private static Set<String> taskReactors = new HashSet<String>();
	static {
		taskReactors.add("RunNumericalCorrelation");
		taskReactors.add("RunMatrixRegression");
		taskReactors.add("RunClassification");
		taskReactors.add("GetRFResults");
		taskReactors.add("RunAssociatedLearning");
		taskReactors.add("NodeDetails");
	}

	// set of reactors that are used to send back ornaments w/ a task to a panel
	private static Set<String> taskOrnamentReactors = new HashSet<String>();
	static {
		taskOrnamentReactors.add("RetrievePanelColorByValue");
	}

	// gson variable
	private static Gson gson = new Gson();

	// keep track of the index of expressions
	private int index = 0;

	// map the index to the expressions
	private HashMap<Integer, String> expressionMap = new HashMap<Integer, String>();

	// list to maintain what sheets exist
	private List<String> sheetList = new Vector<String>();
	
	// keep order of panel creation
	private List<String> panelCreationOrder = new ArrayList<String>();

	// panelMap will keep track of the panelId -> list of expressions in order of occurrence
	// "0" -> [0,1,3] means that panel zero occurred in the TaskOptions of expressions 0, 1 and 3
	private HashMap<String, List<Integer>> panelMap = new HashMap<String, List<Integer>>();

	// layerMap will keep track of the panelId -> layer -> list of expressions in order of occurence
	private HashMap<String, Map<String, List<Integer>>> layerMap = new HashMap<String, Map<String, List<Integer>>>();

	// panelOrnamentMap will track panelId -> list of expressions in order of occurrence
	// "0" -> [5.6] means that panel zero has these panel ornament tasks that need to be tracked
	private HashMap<String, List<Integer>> panelOrnamentMap = new HashMap<String, List<Integer>>();

	// keep a list of the expressions to keep
	// we will add all expressions with no TaskOptions and then add the expressions that we do need with the task options
	// we will order it at the end
	private List<Integer> optimizedRecipeIndicesToKeep = new ArrayList<Integer>();

	// we need to keep track of whether each expression contained TaskOptions
	// create a variable that will be reset for each expression to indicate whether or not we encountered an instance of TaskOptions
	// this is needed because if we get through a whole expression without hitting TaskOptions then we want to go ahead and add it to expressionsToKeep
	private boolean containsTaskOptions = false;
	private boolean containsOrnamentTaskOptions = false;
	private boolean containsPanelKey = false;

	// need to account for clones
	// so grab the most recent Panel() value
	// in case a clone comes afterwards
	private String curPanelId = null;
	private Map<String, String> clonePanelToOrigPanel = new HashMap<String, String>();
	private Map<String, Integer> cloneToOrigTask = new HashMap<String, Integer>();

	private Map<Integer, String> cloneIndexToClonePanelId = new HashMap<Integer, String>();
	private Map<String, Integer> cloneIdToIndex = new HashMap<String, Integer>();

	// need to keep track
	// of the panel view modifications performed
	// so that we know when a panel clone was necessary
	private Map<String, List<Object[]>> panelToPanelView = new HashMap<String, List<Object[]>>();

	// create a variable to keep track of the current mapping of the original expression to the encoded expression
	public HashMap<String, String> encodedToOriginal = new HashMap<String, String>();

	// just aggregate all the remove layer indices for now
	private List<Integer> removeLayerIndices = new Vector<Integer>();
	
	private String insightConfig = null;
	@Deprecated
	private String insightGoldenLayout = null;
	
	/**
	 * This method overrides caseAConfiguration, adds each expression to the expressionMap, adds expression indexes to the expression map, and updates the index
	 * 
	 * @param node
	 */
	@Override
	public void caseARoutineConfiguration(ARoutineConfiguration node) {
		List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
		for(PRoutine e : copy) {
			// for each expression, reset whether it contains TaskOptions
			// default to false and change to true if we hit TaskOptions within the expression
			curPanelId = null;
			containsTaskOptions = false; 
			containsOrnamentTaskOptions = false;
			String expression = e.toString();

			// add the expression to the map
			//when we put the expression in the expression map, this is when we should change to the unencoded version
			expression = PixelUtility.recreateOriginalPixelExpression(expression, encodedToOriginal);
			expressionMap.put(index, expression);
			LOGGER.info("Processing " + expression + "at index: " + index);
			e.apply(this);
			// if we made it through all reactors of the expression without ever hitting TaskOptions, go ahead and add the expression to expressionsToKeep
			// this is run for each expression
			if (!containsTaskOptions && !containsOrnamentTaskOptions) {
				optimizedRecipeIndicesToKeep.add(index);
			}

			// increase the index for each expression 
			index++;
		}
	}

	/**
	 * This method overrides inAOperation and determines the actions to take based on the reactor (TaskOptions and ClosePanel reactors)
	 * 
	 * @param node
	 */
	@Override
	public void inAOperation(AOperation node) {
		defaultIn(node);

		// check to see if the reactor is TaskOptions by getting the reactorId
		String reactorId = node.getId().toString().trim();

		// if the reactor is TaskOptions, we need to know the panel
		if (reactorId.equals("TaskOptions")) {
			// go ahead and set containsTaskOptions to true
			containsTaskOptions = true;
			// input is the whole input to TaskOptions
			POpInput input = node.getOpInput();
			String inputMapString = input.toString();

			// convert the inputMapString to a map
			Map<String, Object> inputMap = null;
			try {
				inputMap = gson.fromJson(inputMapString, Map.class);
			} catch (Exception e2) {
				throw new IllegalArgumentException("Unable to cast TaskOptions into a map");
			}

			// create a TaskOptions object from the map
			// this will allow us to get the panel id
			TaskOptions ops = new TaskOptions(inputMap);
			// it is possible that we have more than panel id passed into TaskOptions
			// the panel ids are stored as a set of strings 
			Set<String> panelId = ops.getPanelIds();

			// we now have a set of panel ids
			// we need to make sure that each of the panel ids is included in panelMap with the associated expression index
			// for each panel in the set of panel ids
			for (String panel : panelId) {
				addPanelForCurrentIndex(panel);

				Map<String, Object> thisPanelOptions = (Map<String, Object>) inputMap.get(panel);
				if(thisPanelOptions == null) {
					// try casting to integer
					// annoying...
					try {
						thisPanelOptions = (Map<String, Object>) inputMap.get(Integer.parseInt(panel + ""));
					} catch(Exception e) {
						//ignore
					}
				}
				if(thisPanelOptions != null && thisPanelOptions.containsKey("layer")) {
					Map<String, Object> layerMap = (Map<String, Object>) thisPanelOptions.get("layer");
					String layerId = (String) layerMap.get("id");
					addLayerForCurrentIndex(panel, layerId);
				} else {
					addLayerForCurrentIndex(panel, "base");
				}
			}
		} else if (reactorId.equals("ClosePanel")) {
			// if we close the panel, then we can get rid of the places we had TaskOptions for that panel
			// the TaskOptions so far are kept track of in panel map
			// first, grab the input from the ClosePanel reactor
			POpInput closePanelInput = node.getOpInput();
			String closePanelId = closePanelInput.toString().trim();

			// we now have the panel id of the closed panel
			// look at the current panel map and see if that key exists
			if (panelMap.containsKey(closePanelId)) {
				// if it exists, then remove it, we don't need any of these expressions
				panelMap.remove(closePanelId);
			}
			// same with panel to panel view map
			if (panelToPanelView.containsKey(closePanelId)) {
				panelToPanelView.remove(closePanelId);
			}
			// remove the panel that was created
			if (panelCreationOrder.contains(closePanelId)) {
				panelCreationOrder.remove(closePanelId);
			}
			// remove the panel from the layer logic as well
			if (layerMap.containsKey(closePanelId)) {
				layerMap.remove(closePanelId);
			}
		} else if(reactorId.equals("Panel")) {
			// this is in case there is a clone that is coming up
			POpInput input = node.getOpInput();
			curPanelId = input.toString().trim();
			curPanelId = trimQuotes(curPanelId);
		}
		else if (reactorId.equals("Clone")) {
			POpInput closePanelInput = node.getOpInput();
			String panel = closePanelInput.toString().trim();
			panel = trimQuotes(panel);
			addPanelForCurrentIndex(panel);
			addLayerForCurrentIndex(panel, "base");

			// store the order of the creation
			panelCreationOrder.add(panel);

			// in the case when you cloned but the original panel has been modified
			// we dont want to clone the new modification
			// but the original
			// so we need to keep track of the original of the clone in case it is used

			// first, find the index of the current panel we are cloning from
			// this is why we keep track of the curPanelId
			Integer lastIndex = getLastPixelTaskIndexForPanel(curPanelId);
			cloneToOrigTask.put(panel, lastIndex);
			clonePanelToOrigPanel.put(panel, curPanelId);

			// store the index of the clone to the panel created
			cloneIndexToClonePanelId.put(index, panel);
			cloneIdToIndex.put(panel, index);
		} 
		else if (reactorId.equals("SetPanelView")) {
			// here, if we did a clone
			// but later changed the panel view
			// i want to no longer keep the clone as part of the optimized recipe
			String view = node.getOpInput().toString().trim();
			if(view.startsWith("\"")) {
				view = view.substring(1);
			}
			if(view.endsWith("\"")) {
				view = view.substring(0, view.length()-1);
			}
			addPanelView(curPanelId, view);
		}  
		// account for order of panel creation
		else if(reactorId.equals("AddPanel")) {
			// store order of panel creation
			POpInput input = node.getOpInput();
			String panel = input.toString().trim();
			panel = trimQuotes(panel);
			panelCreationOrder.add(panel);
		}
		// account for new panel creation
		else if(reactorId.equals("AddPanelIfAbsent")) {
			// store order of panel creation
			POpInput input = node.getOpInput();
			String panel = input.toString().trim();
			panel = trimQuotes(panel);
			if(!panelCreationOrder.contains(panel)) {
				panelCreationOrder.add(panel);
			}
		}
		// remove the layer
		else if(reactorId.equals("RemoveLayer")) {
			removeLayerIndices.add(this.index);
			
//			String panelStr = node.getOpInput().toString().trim();
//			String layerStr = node.getOtherOpInput().get(0).toString().trim();
//			
//			if(panelStr.contains("panel = ")) {
//				panelStr = panelStr.replace("panel =", "").trim();
//				panelStr = trimQuotes(panelStr.replace("[", "").replace("]", "").trim());
//			}
//			if(layerStr.contains(", layer = ")) {
//				layerStr = layerStr.replaceAll(", layer =", "").trim();
//				layerStr = trimQuotes(layerStr.replace("[", "").replace("]", "").trim());
//			}
//			
//			Map<String, List<Integer>> thisLayer = layerMap.get(panelStr);
//			if(thisLayer != null) {
//				thisLayer.remove(layerStr);
//			}
		}
		// need to account for auto tasks
		// need to account for algorithms that just send data to the FE directly
		else if(reactorId.equals("AutoTaskOptions") || taskReactors.contains(reactorId) ) {
			// this is a constant task
			// let the inAGeneric and inAWordOrIdScalar handle getting the correct panel id
			containsTaskOptions = true;
		}
		// need to account for task ornaments
		else if(taskOrnamentReactors.contains(reactorId)) {
			// this is a panel ornament task
			// need to account for this differently

			// you can technically passin the panel directly
			// so will first see if a panel has been defined or not
			if(curPanelId != null) {
				addPanelOrnamentTaskForCurrentIndex(curPanelId);
			} else {
				containsOrnamentTaskOptions = true;
			}
		}
		// add a sheet
		else if(reactorId.equals("AddSheet")) {
			POpInput input = node.getOpInput();
			String sheet = input.toString().trim();
			sheet = trimQuotes(sheet);
			sheetList.add(sheet);
		}
		// removing a sheet
		else if(reactorId.equals("CloseSheet")) {
			POpInput input = node.getOpInput();
			String sheet = input.toString().trim();
			sheet = trimQuotes(sheet);
			sheetList.remove(sheet);
		}
		// is this the config layout
		else if(reactorId.equals("SetInsightGoldenLayout")) {
			// input is the golden layout config map
			POpInput input = node.getOpInput();
			String goldenLayoutString = input.toString();
			
			// just make sure it is valid map
//			try {
//				gson.fromJson(goldenLayoutString, Map.class);
				this.insightGoldenLayout = goldenLayoutString;
//			} catch (Exception e2) {
//				e2.printStackTrace();
//			}
		}
		// is this the config layout
		else if(reactorId.equals("SetInsightConfig")) {
			// input is the golden layout config map
			POpInput input = node.getOpInput();
			String insightConfigString = input.toString();
			
			// just make sure it is valid map
//			try {
//				gson.fromJson(insightConfigString, Map.class);
				this.insightConfig = insightConfigString;
//			} catch (Exception e2) {
//				e2.printStackTrace();
//			}
		}
	}

	@Override
	public void inAGeneric(AGeneric node) {
		String key = node.getId().toString().trim();
		if(key.equals(ReactorKeysEnum.PANEL.getKey())) {
			containsPanelKey = true;
		}
	}

	@Override
	public void outAGeneric(AGeneric node) {
		// reset
		containsPanelKey = false;
	}

	@Override
	public void inAScalarRegTerm(AScalarRegTerm node) {
		if(containsTaskOptions && containsPanelKey) {
			// this is the value for a panel
			// store it with the current index
			String panel = node.toString().trim();
			if(panel.startsWith("\"") || panel.startsWith("'")) {
				panel = panel.substring(1);
			}
			if(panel.endsWith("\"") || panel.endsWith("'")) {
				panel = panel.substring(0, panel.length()-1);
			}
			addPanelForCurrentIndex(panel);
			// add for layer
			// should technically drop all the other layers for these
			addLayerForCurrentIndex(panel, "base");
		} else if(containsOrnamentTaskOptions && containsPanelKey) {
			// this is the value for a panel
			// store it with the current index
			String panel = node.toString().trim();
			if(panel.startsWith("\"") || panel.startsWith("'")) {
				panel = panel.substring(1);
			}
			if(panel.endsWith("\"") || panel.endsWith("'")) {
				panel = panel.substring(0, panel.length()-1);
			}
			addPanelOrnamentTaskForCurrentIndex(panel);
		}
	}

	/**
	 * Add the panel and the current index
	 * If first time seeing panel, adds panel key
	 * If already seen panel, adds to the set of indices for this panel
	 * @param panel
	 */
	private void addPanelForCurrentIndex(String panel) {
		panel = panel.trim();
		if (panelMap.keySet().contains(panel)) {
			// if the panel DOES already exist in the panelMap, we just need to add the expression id to the set of values
			// first get the existing set of index values
			List<Integer> indexVals = panelMap.get(panel);
			// next, add the new index value to the list of values
			if(!indexVals.contains(index)) {
				indexVals.add(index);
			}
		} else {
			// if the panel DOES NOT already exist in the panelMap, we need to add it
			List<Integer> indexVals = new ArrayList<Integer>();
			indexVals.add(index);
			panelMap.put(panel, indexVals);
		}
	}

	/**
	 * Add the panel and the current index
	 * If first time seeing panel, adds panel key + layer key
	 * If already seen panel, adds to the set of indices for this panel
	 * @param panel
	 */
	private void addLayerForCurrentIndex(String panel, String layer) {
		panel = panel.trim();
		layer = layer.trim();
		if (layerMap.keySet().contains(panel)) {
			List<Integer> layerIndex = null;
			// if it does exist
			// is this the first layer
			// or a new layer?
			Map<String, List<Integer>> layerIdsMap = layerMap.get(panel);
			// next, add the new index value to the list of values
			if(layerIdsMap.containsKey(layer)) {
				layerIndex = layerIdsMap.get(layer);
			} else {
				layerIndex = new Vector<Integer>();
				layerIdsMap.put(layer, layerIndex);
			}
			if(!layerIndex.contains(index)) {
				layerIndex.add(index);
			}
		} else {
			// panel doesn't exist at all
			List<Integer> layerIndex = new Vector<Integer>();
			Map<String, List<Integer>> layerIdsMap = new HashMap<String, List<Integer>>();
			layerIdsMap.put(layer, layerIndex);
			layerIndex.add(index);
			layerMap.put(panel, layerIdsMap);
		}
	}

	/**
	 * Same as addPanelForCurrentIndex
	 * But for task ornaments
	 * @param panel
	 */
	private void addPanelOrnamentTaskForCurrentIndex(String panel) {
		panel = panel.trim();
		if (panelOrnamentMap.keySet().contains(panel)) {
			// if the panel DOES already exist in the panelMap, we just need to add the expression id to the set of values
			// first get the existing set of index values
			List<Integer> indexVals = panelOrnamentMap.get(panel);
			// next, add the new index value to the list of values
			if(!indexVals.contains(index)) {
				indexVals.add(index);
			}
		} else {
			// if the panel DOES NOT already exist in the panelMap, we need to add it
			List<Integer> indexVals = new ArrayList<Integer>();
			indexVals.add(index);
			panelOrnamentMap.put(panel, indexVals);
		}
	}

	private void addPanelView(String panel, String view) {
		panel = panel.trim();
		if (panelToPanelView.keySet().contains(panel)) {
			// if the panel DOES already exist in the panelToPanelView, we just need to add the new expression
			// of index to view 
			List<Object[]> existingValues = panelToPanelView.get(panel);
			// next, add the new index value to the list of values
			existingValues.add(new Object[]{index, view});
		} else {
			// if the panel DOES NOT already exist in the panelToPanelView, we need to add it
			List<Object[]> existingValues = new ArrayList<Object[]>();
			existingValues.add(new Object[]{index, view});
			panelToPanelView.put(panel, existingValues);
		}
	}

	/**
	 * This method adds expressions to expressionsToKeep based on the panelMap
	 */
	public List<String> finalizeExpressionsToKeep() {
		// now all expressions have been processed
		// we've been keeping track of the expressions associated with each panel
		// we want to grab the last expression index from each panel in the panelMap and add this to expressions to keep
		// expressionsToKeep already contains the expression indexes that did not have TaskOptions
		for(String panelId : panelMap.keySet()) {
			List<Integer> expressionsForPanel = panelMap.get(panelId);
			int lastExpressionIndex = expressionsForPanel.get(expressionsForPanel.size() - 1);

			// however, if we did a clone from a panel
			// and then changed the original panel view
			// the clone should still maintain the original panel view
			// not the new view
			Integer origTaskIndex = cloneToOrigTask.get(panelId);
			if(origTaskIndex != null) {
				// this is from a clone
				// get the original panel
				String origPanel = clonePanelToOrigPanel.get(panelId);
				// get the last task for that panel
				// but i need to account for if this was closed... so null check it
				List<Integer> origMapTaskIndices = panelMap.get(origPanel);
				if(origMapTaskIndices != null) {
					// we need to check to see if the last task for that panel has changed
					Integer currentLastTaskForPanel = origMapTaskIndices.get(origMapTaskIndices.size()-1);
					if(currentLastTaskForPanel != origTaskIndex) {
						// add back the original task
						addToExpressions(origTaskIndex);
						// and add the clone
						// since i need to move over panel properties
						addToExpressions(lastExpressionIndex);
					}
				}
			}

			// even in the above case where if i added a clone
			// i need to move over panel properties
			// so need to keep that clone in the recipe
			addToExpressions(lastExpressionIndex);
		}
		// we've now created our full list of expressionsToKeep
		// we should order expressions to keep
		Collections.sort(optimizedRecipeIndicesToKeep);

		// now that our expressions are sorted, we can add them to our modifiedRecipe variable to return 

		// grab each index from expressions to keep
		// then match the index with the expression using the expressionMap
		// then add the item from the expressionMap to the modifiedRecipe
		List<String> modifiedRecipe = new ArrayList<String>();
		for (int j = 0; j < optimizedRecipeIndicesToKeep.size(); j++) {
			Integer indexToGrab = optimizedRecipeIndicesToKeep.get(j);
			String keepExpression = expressionMap.get(indexToGrab);
			modifiedRecipe.add(keepExpression);
		}

		return modifiedRecipe;
	}

	/**
	 * Get the last task executed for each panel
	 * @return
	 */
	public List<String> getCachedPixelRecipeSteps() {
		// keep all the expressions we want
		List<Integer> expressionsToKeep = new ArrayList<Integer>();

		List<Integer> cloneExpressionsKept = new ArrayList<Integer>();

		PANEL_LAYER_MAP_LOOP : for(String panelId : layerMap.keySet()) {
			Map<String, List<Integer>> thisLayerMap = layerMap.get(panelId);

			// first, check to see if this is a clone
			// if it is, then we need to see 2 things
			// first, did the panel view change after this step
			// if so, we do not need it
			// second, we need to make sure we are cloning the correct step
			// so that the view has the panel at its original view
			// not at a later view
			List<Integer> expressionsForPanel = panelMap.get(panelId);
			int lastPanelExpressionIndex = expressionsForPanel.get(expressionsForPanel.size() - 1);
			if(cloneIndexToClonePanelId.containsKey(lastPanelExpressionIndex)) {

				// fist check - did we change the panel view at a later point
				List<Object[]> panelViews = panelToPanelView.get(panelId);
				// if we cloned and never changed, this might never be recorded
				// so null check it
				if(panelViews != null) {
					for(Object[] pView : panelViews) {
						int setPanelViewIndex = (int) pView[0];
						String view = (String) pView[1];

						// make sure it is at a relevant point in the expression
						if(setPanelViewIndex > lastPanelExpressionIndex) {
							if(!view.equals("visualization")) {
								// okay, we need to ignore this
								// most likely, the panel was cloned to open up 
								// a new one
								// and then something else has been set inside
								// potentially a filter/infographic/text/other widget
								// this is captured in the viz state
								continue PANEL_LAYER_MAP_LOOP;
							}
						}
					}
				}

				// now, if we did a clone from a panel
				// and then changed the original panel view
				// the clone should still maintain the original panel view
				// not the new view
				Integer origTaskIndex = cloneToOrigTask.get(panelId);
				if(origTaskIndex != null) {
					// this is from a clone
					// get the original panel
					String origPanel = clonePanelToOrigPanel.get(panelId);
					// get the last task for that panel
					// but i need to account for if this was closed... so null check it
					Map<String, List<Integer>> origLayerTaskIndices = layerMap.get(origPanel);
					if(origLayerTaskIndices != null) {
						// we need to check to see if the last task for that panel has changed
						for(String origLayerIndex : origLayerTaskIndices.keySet()) {
							List<Integer> origMapTaskIndices = origLayerTaskIndices.get(origLayerIndex);
							Integer currentLastTaskForPanel = origMapTaskIndices.get(origMapTaskIndices.size()-1);
							if(currentLastTaskForPanel > origTaskIndex) {
								// add back the original task
								if(!expressionsToKeep.contains(origTaskIndex)) {
									expressionsToKeep.add(origTaskIndex);

									// do a sneak peak, is this expression a clone
									if(cloneIndexToClonePanelId.containsKey(origTaskIndex)) {
										cloneExpressionsKept.add(origTaskIndex);
									}
								}
							}
						}
					}
				}

				// we know this expression is a clone
				cloneExpressionsKept.add(lastPanelExpressionIndex);
			}

			for(String layerId : thisLayerMap.keySet()) {
				List<Integer> expressionsForLayer = thisLayerMap.get(layerId);

				int lastExpressionIndex = expressionsForLayer.get(expressionsForLayer.size() - 1);
				expressionsToKeep.add(lastExpressionIndex);

				// let us find any panel ornaments to keep
				// so we can flush these as well
				if(panelOrnamentMap.containsKey(panelId)) {
					List<Integer> ornamentIndices = panelOrnamentMap.get(panelId);
					int size = ornamentIndices.size();
					for(int i = size; i > 0; i--) {
						int ornamentIndex = ornamentIndices.get(i-1);
						// if this happened after the last view
						// add it to the list of expressions to keep
						if(ornamentIndex > lastExpressionIndex) {
							expressionsToKeep.add(ornamentIndex);
						}
					}
				}
			}
		}
		
		// add the remove layers
		expressionsToKeep.addAll(this.removeLayerIndices);
		
		// order the expressions
		Collections.sort(expressionsToKeep);

		// store the return cached recipe
		List<String> cacheRecipe = new ArrayList<String>();
		if(sheetList.isEmpty()) {
			// add the default
			cacheRecipe.add("CachedSheet(\"" + Insight.DEFAULT_SHEET_ID + "\", \"" + Insight.DEFAULT_SHEET_LABEL + "\");");
		} else {
			for(int i = 0; i < sheetList.size(); i++) {
				String sheetId = sheetList.get(i);
				cacheRecipe.add("CachedSheet(\"" + sheetId + "\");");
			}
		}
		List<String> addedPanels = new ArrayList<String>();
		// first, add all the cached panel reactors
		for(int i = 0; i < panelCreationOrder.size(); i++) {
			String orderedPanelId = panelCreationOrder.get(i);

			// find if this panel has a kept clone expression
			Integer creationIndex = cloneIdToIndex.get(orderedPanelId);
			// remember, not all panels are clones
			if(creationIndex != null) {
				// but this one is a clone 
				// is it saved
				if(cloneExpressionsKept.contains(creationIndex)) {
					// it is a clone
					// and we have kept the clone adding
					// we cannot load this yet
					continue;
				}
			}

			// we passed all our checks
			// save to load this cached panel at the beginning
			cacheRecipe.add("CachedPanel(\"" + orderedPanelId + "\");");
			addedPanels.add(orderedPanelId);
		}

		for(Integer index : expressionsToKeep) {
			String keepExpression = expressionMap.get(index);

			// is it a clone expression ?
			if(cloneExpressionsKept.contains(index)) {
				// make sure the panel stil exists
				// and wasn't removed
				String thisCachedPanelId = cloneIndexToClonePanelId.get(index);
				String origPanelId = clonePanelToOrigPanel.get(thisCachedPanelId);

				boolean addAndRemoveOrigPanel = !addedPanels.contains(origPanelId);
				if(addAndRemoveOrigPanel) {
					cacheRecipe.add("AddPanel(\"" + origPanelId + "\");");
					cacheRecipe.add("Panel(\"" + origPanelId + "\")|SetPanelView(\"visualization\");");
					Integer origTask = cloneToOrigTask.get(thisCachedPanelId);
					String origExpression = expressionMap.get(origTask);
					cacheRecipe.add(origExpression);
				}
				keepExpression = keepExpression.replace("Clone (", "CachedPanelClone (");
				cacheRecipe.add(keepExpression);
				if(addAndRemoveOrigPanel) {
					cacheRecipe.add("ClosePanel(\"" + origPanelId + "\");");
				}
				// after we do the clone
				// it is save to now load the cached panel details
				cacheRecipe.add("CachedPanel(\"" + thisCachedPanelId + "\");");
				// store the cached panel as an added panel which can now be cloned 
				addedPanels.add(thisCachedPanelId);
			} else {
				cacheRecipe.add(keepExpression);
			}
		}
		// add the golden layout config at the end
		if(this.insightGoldenLayout != null) {
			cacheRecipe.add("SetInsightGoldenLayout(" + this.insightGoldenLayout + ");");
		}
		// add the insight config at the end
		if(this.insightConfig != null) {
			cacheRecipe.add("SetInsightConfig(" + this.insightConfig + ");");
		}

		return cacheRecipe;
	}


	/**
	 * Get the last run task index from the recipe for a specific panel
	 * @param panelId
	 * @return
	 */
	private Integer getLastPixelTaskIndexForPanel(String panelId) {
		List<Integer> taskIndices = panelMap.get(panelId);
		if(taskIndices != null) {
			return taskIndices.get(taskIndices.size()-1);
		}
		return null;
	}

	/**
	 * This method is used for adding expressions to the list of expressionsToKeep by first checking to see if the index is already contained
	 * we don't want to add an expression index twice
	 * 
	 * @param index
	 */
	public void addToExpressions(int index) {
		if (!optimizedRecipeIndicesToKeep.contains(index)) {
			optimizedRecipeIndicesToKeep.add(index);
		}
	}

	private String trimQuotes(String input) {
		if(input.startsWith("\"") || input.startsWith("'")) {
			input = input.substring(1);
		}
		if(input.endsWith("\"") || input.endsWith("'")) {
			input = input.substring(0, input.length()-1);
		}
		return input;
	}

	public static void main(String[] args) {
		String[] recipe = new String[]{"AddPanel ( 0 ) ;",
				"Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>UnfilterFrame(<SelectedColumn>);</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" , \"Sunburst\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>if(IsEmpty(<SelectedValues>), UnfilterFrame(<SelectedColumn>), SetFrameFilter(<SelectedColumn>==<SelectedValues>));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;",
				"Panel ( 0 ) | RetrievePanelEvents ( ) ;",
				"Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;",
				"Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"<encode>{\"app_id\":\"93857bba-5aea-447b-94f4-f9d9179da4da\"}</encode>\" ) ;",
				"CreateFrame ( frameType = [ GRID ] ) .as ( [ 'FRAME228199' ] ) ;",
				"Database ( database = [ \"93857bba-5aea-447b-94f4-f9d9179da4da\" ] ) | Select ( Director , Title , Nominated , Studio , Genre ) .as ( [ Director , Title , Nominated , Studio , Genre ] ) | Join ( ( Title , inner.join , Genre ) , ( Title , inner.join , Nominated ) , ( Title , inner.join , Director ) , ( Title , inner.join , Studio ) ) | Import ( ) ;",
				"Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;",
				"Frame ( ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 500 ) ;",
				"Select ( Director , Genre , Nominated , Studio ) .as ( [ Director , Genre , Nominated , Studio ] ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Director\" , \"Genre\" , \"Nominated\" , \"Studio\" ] } } } ) | Collect ( 500 ) ;",
				"Select ( Director , Genre , Nominated ) .as ( [ Director , Genre , Nominated ] ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Director\" , \"Genre\" , \"Nominated\" ] } } } ) | Collect ( 500 ) ;",
				"Select ( Director , Genre ) .as ( [ Director , Genre ] ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Director\" , \"Genre\" ] } } } ) | Collect ( 500 ) ;",
				"Select ( Director ) .as ( [ Director ] ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Director\" ] } } } ) | Collect ( 500 ) ;",
				"Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;",
				"if ( ( HasDuplicates ( Studio ) ) , ( Select ( Studio , Count ( Title ) ) .as ( [ Studio , CountofTitle ] ) | Group ( Studio ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Column\" , \"alignment\" : { \"label\" : [ \"Studio\" ] , \"value\" : [ \"CountofTitle\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) , ( Select ( Studio , Count ( Title ) ) .as ( [ Studio , CountofTitle ] ) | Group ( Studio ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Column\" , \"alignment\" : { \"label\" : [ \"Studio\" ] , \"value\" : [ \"CountofTitle\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) ) ;" ,
				"Panel ( 0 ) | Clone ( 1 ) ;",
				"Panel ( 0 ) | Clone ( 2 ) ;",
				"Select ( Director ) .as ( [ Director ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Director\" ] } } } ) | Collect ( 500 ) ;",
				//				"if ( ( HasDuplicates ( Genre ) ) , ( Select ( Genre , Count ( Title ) ) .as ( [ Genre , CountofTitle ] ) | Group ( Genre ) | With ( Panel ( 1 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"1\" : { \"layout\" : \"Column\" , \"alignment\" : { \"label\" : [ \"Genre\" ] , \"value\" : [ \"CountofTitle\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) , ( Select ( Genre , Count ( Title ) ) .as ( [ Genre , CountofTitle ] ) | Group ( Genre ) | With ( Panel ( 1 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"1\" : { \"layout\" : \"Column\" , \"alignment\" : { \"label\" : [ \"Genre\" ] , \"value\" : [ \"CountofTitle\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) ) ;",
				"RunNumericalCorrelation ( attributes = [ MovieBudget, Revenue_Domestic, Revenue_International, RottenTomatoes_Audience, RottenTomatoes_Critics] , panel = [ 0 ] ) ;",
				"x = Select(Genre) | Filter(Genre == \"Thriller-Horror\");",
				"Panel(0) | AddPanelColorByValue(name=[\"abc\"], qs=[x], options=[{\"a\":\"b\"}]);",
				"Panel(0) | RetrievePanelColorByValue(name=[\"abc\"]) | Collect(-1);",
				"Select ( Genre , Average ( MovieBudget ) ) .as ( [ Genre , Average_of_MovieBudget ] ) | Group ( Genre ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Column\" , \"alignment\" : { \"label\" : [ \"Genre\" ] , \"value\" : [ \"Average_of_MovieBudget\" ] , \"tooltip\" : [ ] , \"facet\" : [ ] } } } ) | Collect ( 2000 );",  
				"Select ( Genre , Average ( Revenue_Domestic ) ) .as ( [ Genre , Average_of_Revenue_Domestic ] ) | Group ( Genre ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Line\" , \"alignment\" : { \"label\" : [ \"Genre\" ] , \"value\" : [ \"Average_of_Revenue_Domestic\" ] , \"tooltip\" : [ ] , \"facet\" : [ ] }, \"layer\": { \"id\": \"1\", \"addYAxis\": true, \"addXAxis\": true } } } ) | Collect ( 2000 );",  
				"Select ( Genre , Average ( Revenue_International ) ) .as ( [ Genre , Average_of_Revenue_International ] ) | Group ( Genre ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Area\" , \"alignment\" : { \"label\" : [ \"Genre\" ] , \"value\" : [ \"Average_of_Revenue_International\" ] , \"tooltip\" : [ ] , \"facet\" : [ ] }, \"layer\": { \"id\": \"2\", \"addYAxis\": true, \"addXAxis\": true } } } ) | Collect ( 2000 );",
				"Panel ( 0 ) | Clone ( 3 ) ;",
				"RemoveLayer( panel=[\"0\"] , layer=[\"1\"] );",
				"Panel ( 0 ) | Clone ( 4 ) ;",
				"SetInsightGoldenLayout({\"0\":{\"zeroSum\":{\"settings\":{\"hasHeaders\":true,\"constrainDragToContainer\":true,\"reorderEnabled\":true,\"selectionEnabled\":false,\"popoutWholeStack\":false,\"blockedPopoutsThrowError\":true,\"closePopoutsOnUnload\":true,\"showPopoutIcon\":false,\"showMaximiseIcon\":true,\"showCloseIcon\":true,\"responsiveMode\":\"onload\",\"tabOverlapAllowance\":0,\"reorderOnTabMenuClick\":true,\"tabControlOffset\":10},\"dimensions\":{\"borderWidth\":5,\"borderGrabWidth\":15,\"minItemHeight\":10,\"minItemWidth\":10,\"headerHeight\":20,\"dragProxyWidth\":300,\"dragProxyHeight\":200},\"labels\":{\"close\":\"close\",\"maximise\":\"maximise\",\"minimise\":\"minimise\",\"popout\":\"open in new window\",\"popin\":\"pop in\",\"tabDropdown\":\"additional tabs\"},\"content\":[{\"type\":\"row\",\"isClosable\":false,\"reorderEnabled\":true,\"title\":\"\",\"content\":[{\"type\":\"stack\",\"width\":50,\"isClosable\":true,\"reorderEnabled\":true,\"title\":\"\",\"activeItemIndex\":0,\"content\":[{\"labelOverride\":false,\"label\":\"Pipeline\",\"type\":\"component\",\"panelstatus\":\"normalized\",\"sheetId\":\"0\",\"widgetId\":\"SMSSWidget98f8bbe5-7b56-4d8a-a4d7-37c3435f6334___0\",\"panelId\":\"0\",\"componentName\":\"panel\",\"opacity\":100,\"componentState\":{\"widgetId\":\"SMSSWidget98f8bbe5-7b56-4d8a-a4d7-37c3435f6334___0\",\"sheetId\":\"0\"},\"isClosable\":true,\"reorderEnabled\":true,\"title\":\"Pipeline\"}]},{\"type\":\"stack\",\"width\":50,\"isClosable\":true,\"reorderEnabled\":true,\"title\":\"\",\"activeItemIndex\":0,\"content\":[{\"labelOverride\":false,\"label\":\"Pipeline\",\"type\":\"component\",\"panelstatus\":\"normalized\",\"sheetId\":\"0\",\"widgetId\":\"SMSSWidget98f8bbe5-7b56-4d8a-a4d7-37c3435f6334___2\",\"panelId\":\"2\",\"componentName\":\"panel\",\"sheet\":\"0\",\"componentState\":{\"widgetId\":\"SMSSWidget98f8bbe5-7b56-4d8a-a4d7-37c3435f6334___2\",\"sheetId\":\"0\"},\"isClosable\":true,\"reorderEnabled\":true,\"title\":\"Pipeline\"}]}]}],\"isClosable\":true,\"reorderEnabled\":true,\"title\":\"\",\"openPopouts\":[],\"maximisedItemId\":null}}});"
		};

		OptimizeRecipeTranslation translation = new OptimizeRecipeTranslation();
		for (int i = 0; i < recipe.length; i++) {
			String expression = recipe[i];
			// fill in the encodedToOriginal with map for the current expression
			expression = PixelPreProcessor.preProcessPixel(expression.trim(), translation.encodedToOriginal);
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
				// parsing the pixel - this process also determines if expression is syntactically correct
				Start tree = p.parse();
				// apply the translation
				// when we apply the translation, we will change encoded expressions back to their original form
				tree.apply(translation);
				// reset translation.encodedToOriginal for each expression
				translation.encodedToOriginal = new HashMap<String, String>();
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
			}
		}
		// we want to run the finalizeExpressionsToKeep method only after all expressions have been run
		// this way we can find the last expression index used 
		Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
		//		System.out.println(gson.toJson(translation.finalizeExpressionsToKeep()));
		System.out.println(gson.toJson(translation.getCachedPixelRecipeSteps()));
	}

	/////////////////////////////////////////////////////////////////////////
	
	/*
	 * Deprecated Logic
	 */
	
	/**
	 * Get the last task executed for each panel
	 * @return
	 */
	@Deprecated
	public List<String> getPanelCachedPixelRecipeSteps() {
		// keep all the expressions we want
		List<Integer> expressionsToKeep = new ArrayList<Integer>();

		List<Integer> cloneExpressionsKept = new ArrayList<Integer>();

		PANEL_MAP_LOOP : for(String panelId : panelMap.keySet()) {
			List<Integer> expressionsForPanel = panelMap.get(panelId);
			int lastExpressionIndex = expressionsForPanel.get(expressionsForPanel.size() - 1);

			// first, check to see if this is a clone
			// if it is, then we need to see 2 things
			// first, did the panel view change after this step
			// if so, we do not need it
			// second, we need to make sure we are cloning the correct step
			// so that the view has the panel at its original view
			// not at a later view
			if(cloneIndexToClonePanelId.containsKey(lastExpressionIndex)) {

				// fist check - did we change the panel view at a later point
				List<Object[]> panelViews = panelToPanelView.get(panelId);
				// if we cloned and never changed, this might never be recorded
				// so null check it
				if(panelViews != null) {
					for(Object[] pView : panelViews) {
						int setPanelViewIndex = (int) pView[0];
						String view = (String) pView[1];

						// make sure it is at a relevant point in the expression
						if(setPanelViewIndex > lastExpressionIndex) {
							if(!view.equals("visualization")) {
								// okay, we need to ignore this
								// most likely, the panel was cloned to open up 
								// a new one
								// and then something else has been set inside
								// potentially a filter/infographic/text/other widget
								// this is captured in the viz state
								continue PANEL_MAP_LOOP;
							}
						}
					}
				}

				// now, if we did a clone from a panel
				// and then changed the original panel view
				// the clone should still maintain the original panel view
				// not the new view
				Integer origTaskIndex = cloneToOrigTask.get(panelId);
				if(origTaskIndex != null) {
					// this is from a clone
					// get the original panel
					String origPanel = clonePanelToOrigPanel.get(panelId);
					// get the last task for that panel
					// but i need to account for if this was closed... so null check it
					List<Integer> origMapTaskIndices = panelMap.get(origPanel);
					if(origMapTaskIndices != null) {
						// we need to check to see if the last task for that panel has changed
						Integer currentLastTaskForPanel = origMapTaskIndices.get(origMapTaskIndices.size()-1);
						if(currentLastTaskForPanel > origTaskIndex) {
							// add back the original task
							if(!expressionsToKeep.contains(origTaskIndex)) {
								expressionsToKeep.add(origTaskIndex);

								// do a sneak peak, is this expression a clone
								if(cloneIndexToClonePanelId.containsKey(origTaskIndex)) {
									cloneExpressionsKept.add(origTaskIndex);
								}
							}
						}
					}
				}

				// we know this expression is a clone
				cloneExpressionsKept.add(lastExpressionIndex);
			}

			expressionsToKeep.add(lastExpressionIndex);

			// let us find any panel ornaments to keep
			// so we can flush these as well
			if(panelOrnamentMap.containsKey(panelId)) {
				List<Integer> ornamentIndices = panelOrnamentMap.get(panelId);
				int size = ornamentIndices.size();
				for(int i = size; i > 0; i--) {
					int ornamentIndex = ornamentIndices.get(i-1);
					// if this happened after the last view
					// add it to the list of expressions to keep
					if(ornamentIndex > lastExpressionIndex) {
						expressionsToKeep.add(ornamentIndex);
					}
				}
			}
		}
		// order the expressions
		Collections.sort(expressionsToKeep);

		// store the return cached recipe
		List<String> cacheRecipe = new ArrayList<String>();
		List<String> addedPanels = new ArrayList<String>();
		// first, add all the cached panel reactors
		for(int i = 0; i < panelCreationOrder.size(); i++) {
			String orderedPanelId = panelCreationOrder.get(i);

			// find if this panel has a kept clone expression
			Integer creationIndex = cloneIdToIndex.get(orderedPanelId);
			// remember, not all panels are clones
			if(creationIndex != null) {
				// but this one is a clone 
				// is it saved
				if(cloneExpressionsKept.contains(creationIndex)) {
					// it is a clone
					// and we have kept the clone adding
					// we cannot load this yet
					continue;
				}
			}

			// we passed all our checks
			// save to load this cached panel at the beginning
			cacheRecipe.add("CachedPanel(\"" + orderedPanelId + "\");");
			addedPanels.add(orderedPanelId);
		}

		for(Integer index : expressionsToKeep) {
			String keepExpression = expressionMap.get(index);

			// is it a clone expression ?
			if(cloneExpressionsKept.contains(index)) {
				// make sure the panel stil exists
				// and wasn't removed
				String thisCachedPanelId = cloneIndexToClonePanelId.get(index);
				String origPanelId = clonePanelToOrigPanel.get(thisCachedPanelId);

				boolean addAndRemoveOrigPanel = !addedPanels.contains(origPanelId);
				if(addAndRemoveOrigPanel) {
					cacheRecipe.add("AddPanel(\"" + origPanelId + "\");");
					cacheRecipe.add("Panel(\"" + origPanelId + "\")|SetPanelView(\"visualization\");");
					Integer origTask = cloneToOrigTask.get(thisCachedPanelId);
					String origExpression = expressionMap.get(origTask);
					cacheRecipe.add(origExpression);
				}
				keepExpression = keepExpression.replace("Clone (", "CachedPanelClone (");
				cacheRecipe.add(keepExpression);
				if(addAndRemoveOrigPanel) {
					cacheRecipe.add("ClosePanel(\"" + origPanelId + "\");");
				}
				// after we do the clone
				// it is save to now load the cached panel details
				cacheRecipe.add("CachedPanel(\"" + thisCachedPanelId + "\");");
				// store the cached panel as an added panel which can now be cloned 
				addedPanels.add(thisCachedPanelId);
			} else {
				cacheRecipe.add(keepExpression);
			}
		}

		return cacheRecipe;
	}
}
