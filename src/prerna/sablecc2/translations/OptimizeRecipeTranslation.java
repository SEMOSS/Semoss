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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.AConfiguration;
import prerna.sablecc2.node.AGeneric;
import prerna.sablecc2.node.AOperation;
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
	
	// keep track of the index of expressions
	private int index = 0;
	
	// map the index to the expressions
	private HashMap<Integer, String> expressionMap = new HashMap<Integer, String>();
	
	// keep order of panel creation
	private List<String> panelCreationOrder = new ArrayList<String>();
	
	// panelMap will keep track of the panelId -> list of expressions in order of occurrence
	// "0" -> [0,1,3] means that panel zero occurred in the TaskOptions of expressions 0, 1 and 3
	private HashMap<String, List<Integer>> panelMap = new HashMap<String, List<Integer>>();
	
	// keep a list of the expressions to keep
	// we will add all expressions with no TaskOptions and then add the expressions that we do need with the task options
	// we will order it at the end
	private List<Integer> expressionsToKeep = new ArrayList<Integer>();
	
	// we need to keep track of whether each expression contained TaskOptions
	// create a variable that will be reset for each expression to indicate whether or not we encountered an instance of TaskOptions
	// this is needed because if we get through a whole expression without hitting TaskOptions then we want to go ahead and add it to expressionsToKeep
	private boolean containsTaskOptions = false; 
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
    			addPanelForCurrentIndex(panel);
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
		} else if(reactorId.equals("Panel")) {
			// this is in case there is a clone that is coming up
			POpInput input = node.getOpInput();
        	curPanelId = input.toString().trim();
		}
        else if (reactorId.equals("Clone")) {
			POpInput closePanelInput = node.getOpInput();
			String panel = closePanelInput.toString().trim();
			addPanelForCurrentIndex(panel);
			
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
        // need to account for auto tasks
        // need to account for algorithms that just send data to the FE directly
        else if(reactorId.equals("AutoTaskOptions") || taskReactors.contains(reactorId) ) {
        	// this is a constant task
        	// let the inAGeneric and inAWordOrIdScalar handle getting the correct panel id
        	containsTaskOptions = true;
        } 
        // account for order of panel creation
        else if(reactorId.equals("AddPanel")) {
			// store order of panel creation
			POpInput input = node.getOpInput();
        	String panel = input.toString().trim();
        	panelCreationOrder.add(panel);
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
		}
	}
	
	/**
	 * Add the panel and the current index
	 * If first time seeing panel, adds panel key
	 * If already seen panel, adds to the set of indicies for this panel
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
       Collections.sort(expressionsToKeep);
       
       // now that our expressions are sorted, we can add them to our modifiedRecipe variable to return 
       
       // grab each index from expressions to keep
       // then match the index with the expression using the expressionMap
       // then add the item from the expressionMap to the modifiedRecipe
       List<String> modifiedRecipe = new ArrayList<String>();
       for (int j = 0; j < expressionsToKeep.size(); j++) {
			Integer indexToGrab = expressionsToKeep.get(j);
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
			} else {
				cacheRecipe.add(keepExpression);
			}
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
		if (!expressionsToKeep.contains(index)) {
			expressionsToKeep.add(index);
		}
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
				"Select ( Director ) .as ( [ Director ] ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Director\" ] } } } ) | Collect ( 500 ) ;",
//				"if ( ( HasDuplicates ( Genre ) ) , ( Select ( Genre , Count ( Title ) ) .as ( [ Genre , CountofTitle ] ) | Group ( Genre ) | With ( Panel ( 1 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"1\" : { \"layout\" : \"Column\" , \"alignment\" : { \"label\" : [ \"Genre\" ] , \"value\" : [ \"CountofTitle\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) , ( Select ( Genre , Count ( Title ) ) .as ( [ Genre , CountofTitle ] ) | Group ( Genre ) | With ( Panel ( 1 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"1\" : { \"layout\" : \"Column\" , \"alignment\" : { \"label\" : [ \"Genre\" ] , \"value\" : [ \"CountofTitle\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) ) ;",
				"RunNumericalCorrelation ( attributes = [ MovieBudget, Revenue_Domestic, Revenue_International, RottenTomatoes_Audience, RottenTomatoes_Critics] , panel = [ 0 ] ) ;"
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
		System.out.println(gson.toJson(translation.finalizeExpressionsToKeep()));
		System.out.println(gson.toJson(translation.getCachedPixelRecipeSteps()));
	}

}
