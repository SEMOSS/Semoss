package prerna.om;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.reactor.export.IFormatter;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.gson.GsonUtility;

public class InsightPanel {

	private static Gson GSON = GsonUtility.getDefaultGson();
	
	// unique id for the panel
	private String panelId;
	// the sheet this panel sits in
	private String sheetId;
	// label for the panel
	private String panelLabel;
	// current UI view for the panel
	private String view;
	// active view options
	private String viewOptions;
	// rendered view options
	// for things like KPI
	private String renderedViewOptions;
	private List<String> dynamicVars;
	
	// panel configuration - opacity, etc.
	private Map<String, Object> config;
	// view options on the current view
	private transient Map<String, Map<String, Object>> viewOptionsMap;
	// state held for UI options on the panel
	private transient Map<String, Object> ornaments;
	// state held for events on the panel
	private transient Map<String, Object> events;
	// set of filters that are only applied to this panel
	private transient GenRowFilters grf;
	// set the sorts on the panel
	private transient List<IQuerySort> orderBys;
	// list of comments added to the panel
	// key is the id pointing to the info on the comment
	// the info on the comment also contains the id
	private transient Map<String, Map<String, Object>> comments;
	
	// store the color by value rules for the panel
	private transient List<ColorByValueRule> colorByValue;
	
	// last qs
	private transient SelectQueryStruct lastQs = null;
	// last task options
	private transient TaskOptions lastTaskOptions = null;
	// last formatter
	private transient IFormatter lastFormatter = null;
	// mapping from layer to task
	private transient Map<String, TaskOptions> layerTaskOption = null;
	private transient Map<String, SelectQueryStruct> layerQueryStruct = null;
	private transient Map<String, IFormatter> layerFormatter = null;

	// the default # to collect from tasks on this panel
	private int numCollect = 2000;

	// set temporary filter model state frame
	private transient ITableDataFrame tempFitlerModelFrame;
	// set temporary filter model state
	private transient GenRowFilters tempFilterModelGrf;

	public InsightPanel(String panelId, String sheetId) {
		this.panelId = panelId;
		this.sheetId = sheetId;
		this.viewOptionsMap = new HashMap<String, Map<String, Object>>();
		this.config = new HashMap<String, Object>();
		this.ornaments = new HashMap<String, Object>();
		this.events = new HashMap<String, Object>();
		this.comments = new HashMap<String, Map<String, Object>>();
		
		this.colorByValue = new ArrayList<ColorByValueRule>();
		this.grf = new GenRowFilters();
		this.orderBys = new ArrayList<IQuerySort>();
		
		this.tempFilterModelGrf = new GenRowFilters();
	}
	
	/**
	 * Merge new panel configuration options into the existing panel config map
	 * This will merge child maps together if possible 
	 * @param config
	 */
	public void addConfig(Map<String, Object> newPanelConfig) {
		recursivelyMergeMaps(this.config, newPanelConfig);
	}
	
	/**
	 * Get the panel config
	 * @return
	 */
	public Map<String, Object> getConfig() {
		return this.config;
	}
	
	/**
	 * Get the color by value rules
	 * @return
	 */
	public List<ColorByValueRule> getColorByValue() {
		return this.colorByValue;
	}
	
	/**
	 * Add a new cbv rule
	 * @param cbv
	 */
	public void addColorByValue(ColorByValueRule cbv) {
		// if the name is the same
		// we will override
		// we defined the equals to do this
		if(this.colorByValue.contains(cbv)) {
			this.colorByValue.remove(cbv);
		}
		this.colorByValue.add(cbv);
	}
	
	/**
	 * Get a specific color by value rule
	 * @param ruleId
	 * @return
	 */
	public ColorByValueRule getColorByValue(String ruleId) {
		for(ColorByValueRule cbv : this.colorByValue) {
			if(cbv.getId().equals(ruleId)) {
				return cbv;
			}
		}
		return null;
	}
	
	/**
	 * Delete a specific color by value rule
	 * @param ruleId
	 * @return
	 */
	public boolean removeColorByValue(String ruleId) {
		for(int i = 0; i < this.colorByValue.size(); i++) {
			ColorByValueRule cbv = this.colorByValue.get(i);
			if(cbv.getId().equals(ruleId)) {
				this.colorByValue.remove(i);
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Get the ornaments on the panel
	 * @return
	 */
	public Map<String, Object> getOrnaments() {
		return this.ornaments;
	}
	
	/**
	 * Merge new ornaments into the existing ornament map
	 * This will merge child maps together if possible
	 * @param ornaments
	 */
	public void addOrnaments(Map<String, Object> newOrnaments) {
		recursivelyMergeMaps(this.ornaments, newOrnaments);
	}
	
	/**
	 * Remove all ornaments downstream from a given traversal path
	 * @param traversal
	 * @return
	 */
	public boolean removeOrnament(String traversal) {
		return removePathFromMap(this.ornaments, traversal);
	}
	
	/**
	 * Remove all ornaments
	 */
	public void resetOrnaments() {
		this.ornaments.clear();
	}
	
	/**
	 * Get the events on the panel
	 * @return
	 */
	public Map<String, Object> getEvents() {
		return this.events;
	}
	
	/**
	 * Merge new event info into the existing event map
	 * This will merge child maps together if possible
	 * @param ornaments
	 */
	public void addEvents(Map<String, Object> newEventInfo) {
		recursivelyMergeMaps(this.events, newEventInfo);
	}
	
	/**
	 * Remove all event info downstream from a given traversal path
	 * @param traversal
	 * @return
	 */
	public boolean removeEvent(String traversal) {
		return removePathFromMap(this.events, traversal);
	}
	
	/**
	 * Remove all event info
	 */
	public void resetEvents() {
		this.events.clear();
	}
	
	/**
	 * traversalPath is a period delimited key traversal to go through the ornaments object
	 * @param traversalPath
	 * @return
	 */
	public Object getMapInput(Map<String, Object> map, String traversalPath) {
		String[] traversal = traversalPath.split("\\.");

		// check first key
		if(!map.containsKey(traversal[0])) {
			return null;
		}
		Object innerObj = map.get(traversal[0]);
		
		// go to the necessary depth required
		int depth = traversal.length;
		for(int i = 1; i < depth; i++) {
			if(innerObj instanceof Map) {
				Map innerMap = (Map) innerObj;
				if(innerMap.containsKey(traversal[i])) {
					innerObj = innerMap.get(traversal[i]);
				} else {
					// well, can't find this...
					// just return empty
					return null;
				}
			}
			else {
				// well, can't find this...
				// just return empty
				return null;
			}
		}
		
		return innerObj;
	}
	
	/**
	 * Return the map of additional formatting stored on the panel for export functionality
	 * @param formatDatavalues
	 * @return
	 */
	public Map<String , Map<String,String>> getPanelFormatValues() {
		Map<String , Map<String,String>> res = new HashMap<String , Map<String,String>>();
		Object formatDataValues = getMapInput(this.ornaments, "tools.shared.formatDataValues.formats");
		if (formatDataValues != null && formatDataValues instanceof List) {
			List<Object> arr = (List<Object>) formatDataValues;
			for (int i = 0; i < arr.size(); i++) {
				if (arr.get(i) instanceof Map) {
					Map innerMap = (Map) arr.get(i);
					if(innerMap.get("dimension") != null) {
						Map<String,String> resultMap = new HashMap<String, String>();
						if (innerMap.get("type") != null) {
							resultMap.put("type", innerMap.get("type").toString().toLowerCase());
						}
						if (innerMap.get("prepend") != null && !(innerMap.get("prepend").toString().isEmpty())) {
							resultMap.put("prepend", innerMap.get("prepend").toString());
						}
						if (innerMap.get("append") != null && !(innerMap.get("append").toString().isEmpty())) {
							resultMap.put("append", innerMap.get("append").toString());
						}
						if (innerMap.get("round") != null && !(innerMap.get("round").toString().isEmpty())) {
							resultMap.put("round", innerMap.get("round").toString());
						}
						if (innerMap.get("delimiter") != null && !(innerMap.get("delimiter").toString().isEmpty())) {
							resultMap.put("delimiter", innerMap.get("delimiter").toString());
						}
						
						if (innerMap.get("dimensionType") != null 
								&& innerMap.get("date") != null 
								&& ( innerMap.get("dimensionType").toString().equalsIgnoreCase("Date") 
								|| innerMap.get("dimensionType").toString().equalsIgnoreCase("TIMESTAMP"))
								&& !innerMap.get("date").toString().isEmpty()
								) {
							resultMap.put("dateType", innerMap.get("date").toString());
						}
						// only add is result map is not empty
						if(!resultMap.isEmpty()) {
							res.put(innerMap.get("dimension").toString(), resultMap);
						}
					}
				}
			}
		}
		return res;
	}
	
	/**
	 * Remove all ornaments downstream from a given traversal path
	 * @param traversal
	 * @return
	 */
	private boolean removePathFromMap(Map<String, Object> map, String traversal) {
		String[] path = traversal.split("\\.");
		int size = path.length - 1;
		Map<String, Object> removeInnerMap = map;
		for(int i = 0; i < size; i++) {
			if(removeInnerMap.containsKey(path[i])) {
				Object objVal = removeInnerMap.get(path[i]);
				if(objVal instanceof Map) {
					removeInnerMap = (Map<String, Object>) objVal;
				} else {
					// well, i can only remove if it is a key in a map
					// cannot remove, will just break and set the inner map to null
					removeInnerMap = null;
					break;
				}
			} else {
				// well, your traversal is invalid
				// cannot remove, will just break and set the inner map to null
				removeInnerMap = null;
				break;
			}
		}
		
		// now, i will try to remove the last portion of the path
		if(removeInnerMap != null && removeInnerMap.containsKey(path[size])) {
			removeInnerMap.remove(path[size]);
			// return true for successfully able to remove
			return true;
		} else {
			// return false for not being able to remove
			return false;
		}
	}
	
	private void recursivelyMergeMaps(Map<String, Object> mainMap, Map<String, Object> newMap) {
		if(newMap != null) {
			for(String key : newMap.keySet()) {
				if(mainMap.containsKey(key)) {
					// we have an overlap
					// lets see if the children are both maps
					boolean newKeyIsMap = (newMap.get(key) instanceof Map);
					boolean existingKeyIsMap = (mainMap.get(key) instanceof Map);
					if(newKeyIsMap && existingKeyIsMap) {
						// recursively go through and try to add
						recursivelyMergeMaps( (Map) mainMap.get(key), (Map) newMap.get(key));
					} else {
						// both are not maps
						// just override
						mainMap.put(key, newMap.get(key));
					}
				} else {
					// brand new key
					// put all into the main map
					mainMap.put(key, newMap.get(key));
				}
			}
		}
	}
	
	public void setComments(Map<String, Map<String, Object>> comments) {
		this.comments = comments;
	}
	
	/**
	 * Insert a new comment on the panel
	 * @param comment
	 */
	public void addComment(Map<String, Object> comment) {
		if(!comment.containsKey("id")) {
			throw new IllegalArgumentException("Comment must have a defined id");
		}
		String commentId = comment.get("id").toString();
		this.comments.put(commentId, comment);
	}
	
	/**
	 * Update an existing comment on the panel
	 * @param comment
	 */
	public void updateComment(Map<String, Object> comment) {
		if(!comment.containsKey("id")) {
			throw new IllegalArgumentException("Comment must have a defined id");
		}
		String commentId = comment.get("id").toString();
		if(!this.comments.containsKey(commentId)) {
			throw new IllegalArgumentException("Could not find comment with id = " + commentId);
		}
		this.comments.put(commentId, comment);
	}
	
	/**
	 * Delete a specific comment on the panel
	 * @param id
	 */
	public void removeComment(String commentId) {
		if(!this.comments.containsKey(commentId)) {
			throw new IllegalArgumentException("Could not find comment with id = " + commentId);
		}
		this.comments.remove(commentId);
	}
	
	public Map<String, Object> getComment(String commentId) {
		if(!this.comments.containsKey(commentId)) {
			throw new IllegalArgumentException("Could not find comment with id = " + commentId);
		}
		return this.comments.get(commentId);
	}
	
	public Map<String, Map<String, Object>> getComments() {
		return this.comments;
	}
	
	/**
	 * Delete all comments on the panel
	 */
	public void resetComments() {
		this.comments.clear();
	}
	
	public String getPanelId() {
		return this.panelId;
	}
	
	public void setPanelId(String panelId) {
		this.panelId = panelId;
	}
	
	public void setPanelLabel(String panelLabel) {
		this.panelLabel = panelLabel;
	}
	
	public String getPanelLabel() {
		return this.panelLabel;
	}
	
	public String getSheetId() {
		return this.sheetId;
	}
	
	public void setSheetId(String sheetId) {
		this.sheetId = sheetId;
	}
	
	public void setPanelView(String view) {
		this.view = view;
		// even if the view is not visualization
		// we will maintain the last qs / task options
		// so the user can toggle back to visualization mode
//		if(this.view != null && !this.view.equals("visualization")) {
//			// null out the qs
//			// since we do not want to accidently paint over 
//			// the current display (pipeline, filter, etc.)
//			this.lastQs = null;
//			this.taskOptions = null;
//			this.layerTaskOption = null;
//		}
		
		// set the current view options
		Map<String, Object> thisViewMap = this.viewOptionsMap.get(view);
		// set the current view options
		if(thisViewMap == null) {
			this.viewOptions = null;
		} else {
			this.viewOptions = GSON.toJson(thisViewMap);
		}
	}

	public String getPanelView() {
		return this.view;
	}
	
	/**
	 * Append new view options for the given view
	 * @param view
	 * @param viewOptions
	 */
	public void appendPanelViewOptions(String view, Map<String, Object> viewOptions) {
		if(viewOptions != null && !viewOptions.isEmpty()) {
			// view options is append only
			if(this.viewOptionsMap.containsKey(view)) {
				Map<String, Object> thisViewMap = this.viewOptionsMap.get(view);
				thisViewMap.putAll(viewOptions);
			} else {
				this.viewOptionsMap.put(view, viewOptions);
			}
		}
		
		Map<String, Object> thisViewMap = this.viewOptionsMap.get(view);
		// set the current view options
		if(thisViewMap == null) {
			this.viewOptions = null;
		} else {
			this.viewOptions = GSON.toJson(thisViewMap);
		}
	}
	
	public void setPanelActiveViewOptions(String viewOptions) {
		this.viewOptions = viewOptions;
	}
	
	public String getPanelActiveViewOptions() {
		return this.viewOptions;
	}
	
	public Map<String, Map<String, Object>> getPanelViewOptions() {
		return this.viewOptionsMap;
	}
	
	public void setPanelViewOptions(Map<String, Map<String, Object>> viewOptions) {
		if(viewOptions != null) {
			this.viewOptionsMap = viewOptions;
		}
		
		// set the current view options
		Map<String, Object> thisViewMap = this.viewOptionsMap.get(view);
		if(thisViewMap != null) {
			this.viewOptions = GSON.toJson(thisViewMap);
		}
	}
	
	public String getRenderedViewOptions() {
		return renderedViewOptions;
	}

	public void setRenderedViewOptions(String renderedViewOptions, List<Object> dynamicVarNames) {
		this.renderedViewOptions = renderedViewOptions;
		this.dynamicVars = new ArrayList<>();
		if(dynamicVarNames != null) {
			for(Object obj : dynamicVarNames) {
				if(obj instanceof String) {
					this.dynamicVars.add((String) obj); 
				} else if(obj instanceof Variable) {
					this.dynamicVars.add( ((Variable) obj).getName());
				}
			}
		}
	}
	
	public List<String> getDynamicVars() {
		return this.dynamicVars;
	}

	/**
	 * Return the panel level filters
	 * @return
	 */
	public GenRowFilters getPanelFilters() {
		return this.grf;
	}
	
	public void addPanelFilters(GenRowFilters grf) {
		this.grf.merge(grf);
	}
	
	public int getNumCollect() {
		return numCollect;
	}

	public void setNumCollect(int numCollect) {
		this.numCollect = numCollect;
	}
	
	/**
	 * Get the temp filter model grf
	 * @return
	 */
	public GenRowFilters getTempFilterModelGrf() {
		return tempFilterModelGrf;
	}

	/**
	 * Set the temp filter model grf
	 * @param tempFilterModelGrf
	 */
	public void setTempFilterModelGrf(GenRowFilters tempFilterModelGrf) {
		this.tempFilterModelGrf = tempFilterModelGrf;
	}
	
	/**
	 * Get the temp filter model frame
	 * @return
	 */
	public ITableDataFrame getTempFitlerModelFrame() {
		return tempFitlerModelFrame;
	}

	/**
	 * Set the temp filter model frame
	 * @param tempFitlerModelFrame
	 */
	public void setTempFitlerModelFrame(ITableDataFrame tempFitlerModelFrame) {
		this.tempFitlerModelFrame = tempFitlerModelFrame;
	}
	
	/**
	 * Return the panel level sorts
	 * @return
	 */
	public List<IQuerySort> getPanelOrderBys() {
		return this.orderBys;
	}
	
	public void setPanelOrderBys(List<IQuerySort> orderBys) {
		this.orderBys = orderBys;
	}
	
	public TaskOptions getLastTaskOptions() {
		return lastTaskOptions;
	}
	
	public void setLastTaskOptions(TaskOptions lastTaskOptions) {
		this.lastTaskOptions = lastTaskOptions;
	}
	
	public SelectQueryStruct getLastQs() {
		return lastQs;
	}

	public void setLastQs(SelectQueryStruct lastQs) {
		if(this.lastQs != null && this.lastTaskOptions != null) {
			setFinalViewOptions(lastQs, this.lastTaskOptions, this.lastFormatter);
		} else {
			this.lastQs = lastQs;
		}
	}
	
	public IFormatter lastFormatter() {
		return this.lastFormatter;
	}
	
	public void setLastFormatter(IFormatter lastFormatter) {
		this.lastFormatter = lastFormatter;
	}

	public void setFinalViewOptions(SelectQueryStruct lastQs, TaskOptions lastTaskOptions, IFormatter lastFormatter) {
		this.lastQs = lastQs;
		this.lastTaskOptions = lastTaskOptions;
		this.lastFormatter = lastFormatter;
		// grab the panel map
		if(this.lastTaskOptions != null) {
			String layer = this.lastTaskOptions.getPanelLayerId(this.panelId);
			if(layer == null) {
				layer = "0";
			}
			if(this.layerQueryStruct == null) {
				this.layerQueryStruct = new HashMap<>();
			}
			if(this.layerTaskOption == null) {
				this.layerTaskOption = new HashMap<>();
			}
			if(this.layerFormatter == null) {
				this.layerFormatter = new HashMap<>();
			}
			
			this.layerTaskOption.put(layer, lastTaskOptions);
			this.layerQueryStruct.put(layer, lastQs);
			this.layerFormatter.put(layer, lastFormatter);
		}
	}
	
	public void removeLayerViewOptions(String layerId) {
		if(this.layerTaskOption != null) {
			this.layerTaskOption.remove(layerId);
			this.layerQueryStruct.remove(layerId);
			this.layerFormatter.remove(layerId);
		}
	}
	
	public Map<String, TaskOptions> getLayerTaskOption() {
		return this.layerTaskOption;
	}
	
	public void setLayerTaskOptions(Map<String, TaskOptions> layerTaskOption) {
		this.layerTaskOption = layerTaskOption;
	}
	
	public Map<String, SelectQueryStruct> getLayerQueryStruct() {
		return this.layerQueryStruct;
	}
	
	public void setLayerQueryStruct(Map<String, SelectQueryStruct> layerQueryStruct) {
		this.layerQueryStruct = layerQueryStruct;
	}
	
	public Map<String, IFormatter> getLayerFormatter() {
		return layerFormatter;
	}

	public void setLayerFormatter(Map<String, IFormatter> layerFormatter) {
		this.layerFormatter = layerFormatter;
	}

	/**
	 * Take all the properties of another insight panel
	 * and set them for this panel
	 * @param existingPanel
	 */
	public void clone(InsightPanel existingPanel) {
		Gson gson =  GsonUtility.getDefaultGson();
		this.view = existingPanel.view;
		if(existingPanel.panelLabel != null) {
			this.panelLabel = existingPanel.panelLabel + " Clone";
		}
		this.viewOptions = existingPanel.viewOptions;
		this.config.putAll(gson.fromJson(gson.toJson(existingPanel.config), Map.class));
		this.viewOptionsMap.putAll(gson.fromJson(gson.toJson(existingPanel.viewOptionsMap), Map.class));
		this.ornaments.putAll(gson.fromJson(gson.toJson(existingPanel.ornaments), Map.class));
		this.comments.putAll(gson.fromJson(gson.toJson(existingPanel.comments), Map.class));
		this.events.putAll(gson.fromJson(gson.toJson(existingPanel.events), Map.class));
		this.grf = existingPanel.grf.copy();
		// also move over the CBV
		for(ColorByValueRule rule : existingPanel.colorByValue) {
			ColorByValueRule newRule = gson.fromJson(gson.toJson(rule), ColorByValueRule.class);
			this.addColorByValue(newRule);
		}

		// copy the options and the qs too
		if(existingPanel.lastTaskOptions != null) {
			TaskOptions copyTaskOptions = gson.fromJson(gson.toJson(existingPanel.lastTaskOptions), TaskOptions.class);
			// replace the panel in the task options
			copyTaskOptions.swapPanelIds(this.panelId, existingPanel.getPanelId());
			copyTaskOptions.setCollectStore(existingPanel.lastTaskOptions.getCollectStore());
			this.lastTaskOptions = copyTaskOptions;
			this.lastQs = existingPanel.lastQs;
		}

		if(existingPanel.layerQueryStruct != null) {
			this.layerQueryStruct = new HashMap<>();
			for(String layerId : existingPanel.layerQueryStruct.keySet()) {
				SelectQueryStruct thisQs = existingPanel.layerQueryStruct.get(layerId);
				if(thisQs != null) {
					SelectQueryStruct copyQs = gson.fromJson(gson.toJson(thisQs), thisQs.getClass());
					// set the data fields that are not copied over
					copyQs.setFrame(thisQs.getFrame());
					copyQs.setEngine(thisQs.getEngine());
					layerQueryStruct.put(layerId, copyQs);
				}
			}
		}

		if(existingPanel.layerTaskOption != null) {
			this.layerTaskOption = new HashMap<>();
			for(String layerId : existingPanel.layerTaskOption.keySet()) {
				TaskOptions thisTaskOptions = existingPanel.layerTaskOption.get(layerId);
				if(thisTaskOptions != null) {
					TaskOptions copyTaskOptions = gson.fromJson(gson.toJson(thisTaskOptions), TaskOptions.class);
					copyTaskOptions.swapPanelIds(this.panelId, existingPanel.getPanelId());
					copyTaskOptions.setCollectStore(thisTaskOptions.getCollectStore());
					this.layerTaskOption.put(layerId, copyTaskOptions);
				}
			}
		}
		
		// store the formatter information
		if(existingPanel.lastFormatter != null) {
			this.lastFormatter = existingPanel.lastFormatter;
		}
		if(existingPanel.layerFormatter != null) {
			this.layerFormatter = new HashMap<>(existingPanel.layerFormatter);
		}
	}

}
