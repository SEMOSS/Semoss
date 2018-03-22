package prerna.sablecc2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AConfiguration;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.POpInput;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.om.task.options.TaskOptions;

public class OptimizeRecipeTranslation extends DepthFirstAdapter {
	
	// create logger
	private static final Logger LOGGER = LogManager.getLogger(OptimizeRecipeTranslation.class.getName());
	
	// we are going to take recipe and modify it to delete views (pixels with TaskOptions) that aren't needed
	private List<String> modifiedRecipe = new ArrayList<String>();

	// keep track of the index of expressions
	private int index = 0;
	
	// map the index to the expressions
	private HashMap<Integer, String> expressionMap = new HashMap<Integer, String>();
	
	// panelMap will keep track of the panelId -> list of expressions in order of occurrence
	// "0" -> [0,1,3] means that panel zero occurred in the TaskOptions of expressions 0, 1 and 3
	private HashMap<String, ArrayList<Integer>> panelMap = new HashMap<String, ArrayList<Integer>>();
	
	// keep a list of the expressions to keep
	// we will add all expressions with no TaskOptions and then add the expressions that we do need with the task options
	// we will order it at the end
	private ArrayList<Integer> expressionsToKeep = new ArrayList<Integer>();
	
	// we need to keep track of whether each expression contained TaskOptions
	// create a variable that will be reset for each expression to indicate whether or not we encountered an instance of TaskOptions
	// this is needed because if we get through a whole expression without hitting TaskOptions then we want to go ahead and add it to expressionsToKeep
	private boolean containsTaskOptions; 
	
	// create a variable to keep track of the current mapping of the original expression to the encoded expression
	public HashMap<String, String> encodedToOriginal = new HashMap<String, String>();
	
	/**
	 * This method overrides caseAConfiguration, adds each expression to the expressionMap, adds expression indexes to the expression map, and updates the index
	 * 
	 * @param node
	 */
	@Override
	public void caseAConfiguration(AConfiguration node) {
        List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
        for(PRoutine e : copy) {
        	// for each expression, reset whether it contains TaskOptions
        	// default to false and change to true if we hit TaskOptions within the expression
        	containsTaskOptions = false; 
        	String expression = e.toString();
        	// add the expression to the map
        	
        	//when we put the expression in the expression map, this is when we should change to the unencoded version
        	expression = PixelUtility.recreateOriginalPixelExpression(expression, encodedToOriginal);
        	expressionMap.put(index, expression);
        	LOGGER.info("Processing " + expression + "at index: " + index);
        	e.apply(this);
        	// if we made it through all reactors of the expression without ever hitting TaskOptions, go ahead and add the expression to expressionsToKeep
        	// this is run for each expression
        	if (!containsTaskOptions) {
        		expressionsToKeep.add(index);
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
        	//input.getClass();
        	String inputMapString = input.toString();
        	
        	// convert the inputMapString to a map
        	Map<String, Object> inputMap = null;
    		try {
    			inputMap = new ObjectMapper().readValue(inputMapString, Map.class);
    		} catch (IOException e2) {
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
        		// first check to see if the panel already exists in the panel map
        		if (panelMap.keySet().contains(panel)) {
        			// if the panel DOES already exist in the panelMap, we just need to add the expression id to the set of values
        			// first get the existing set of index values
        			ArrayList<Integer> indexVals = panelMap.get(panel);
        			// next, add the new index value to the list of values
        			indexVals.add(index);
        			// add the updated set of index values to the panelMap
        			panelMap.put(panel, indexVals);
        			
        		} else {
        			// if the panel DOES NOT already exist in the panelMap, we need to add it
        			ArrayList<Integer> indexVals = new ArrayList<Integer>();
        			indexVals.add(index);
        			panelMap.put(panel, indexVals);
        		}
        	}
        } else if (reactorId.equals("ClosePanel")) {
        	// if we close the panel, then we can get rid of the places we had TaskOptions for that panel
        	// the TaskOptions so far are kept track of in panel map
        	// first, grab the input from the ClosePanel reactor
        	POpInput closePanelInput = node.getOpInput();
        	String closePanelId = closePanelInput.toString().trim();
        	
        	// we now have the panel id of the closed panel
        	// look at the current panelmap and see if that key exists
        	if (panelMap.containsKey(closePanelId)) {
        		// if it exists, then remove it, we don't need any of these expressions
        		panelMap.remove(closePanelId);
        	}
        }
	}
	
	/**
	 * This method adds expressions to expressionsToKeep based on the panelMap
	 */
	public void finalizeExpressionsToKeep() {
        // now all expressions have been processed
        // we've been keeping track of the expressions associated with each panel
        // we want to grab the last expression index from each panel in the panelMap and add this to expressions to keep
        // expressionsToKeep already contains the expression indexes that did not have TaskOptions
        for (ArrayList<Integer> expressionsForPanel : panelMap.values()) {
        	int lastExpressionIndex = expressionsForPanel.get(expressionsForPanel.size() - 1);
        	addToExpressions(lastExpressionIndex);
        }
        
        // we've now created our full list of expressionsToKeep
        // we should order expressions to keep
       Collections.sort(expressionsToKeep);
       
       // now that our expressions are sorted, we can add them to our modifiedRecipe variable to return 
       
       // grab each index from expressions to keep
       // then match the index with the expression using the expressionMap
       // then add the item from the expressionMap to the modifiedRecipe
		for (int j = 0; j < expressionsToKeep.size(); j++) {
			Integer indexToGrab = expressionsToKeep.get(j);
			String keepExpression = expressionMap.get(indexToGrab);
			modifiedRecipe.add(keepExpression);
		}
	}

	/**
	 * This method returns that modified recipe
	 * 
	 * @return modifiedRecipe
	 */
	public List<String> getModifiedRecipe() {
		return modifiedRecipe;
	}
	
	
	/**
	 * This method is used for adding expressions to the list of expressionsToKeep by first checking to see if the index is already contained
	 * we don't want to add an expression index twice
	 * 
	 * @param index
	 */
	public void addToExpressions(int index) {
		if (!expressionsToKeep.contains(index)) {
			expressionsToKeep.add(index);
		}
	}
}
