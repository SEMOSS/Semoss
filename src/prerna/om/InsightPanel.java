package prerna.om;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.util.gson.ColorByValueRuleAdapter;
import prerna.util.gson.GsonUtility;
import prerna.util.gson.NumberAdapter;

public class InsightPanel {

	private static Gson GSON = GsonUtility.getDefaultGson();
	
	// unique id for the panel
	private String panelId;
	// label for the panel
	private String panelLabel;
	// current UI view for the panel
	private String view;
	// active view options
	private String viewOptions;
	// panel configuration - opacity, etc.
	private Map<String, Object> config;
	// view options on the current view
	private transient Map<String, Map<String, String>> viewOptionsMap;
	// state held for UI options on the panel
	private transient Map<String, Object> ornaments;
	// state held for events on the panel
	private transient Map<String, Object> events;
	// set of filters that are only applied to this panel
	private transient GenRowFilters grf;
	// set the sorts on the panel
	private transient List<QueryColumnOrderBySelector> orderBys;
	// list of comments added to the panel
	// key is the id pointing to the info on the comment
	// the info on the comment also contains the id
	private transient Map<String, Map<String, Object>> comments;
	// map to store the panel position
	private transient Map<String, Object> position;
	
	// store the color by value rules for the panel
	private transient List<ColorByValueRule> colorByValue;
	
	
	public InsightPanel(String panelId) {
		this.panelId = panelId;
		this.viewOptionsMap = new HashMap<String, Map<String, String>>();
		this.config = new HashMap<String, Object>();
		this.ornaments = new HashMap<String, Object>();
		this.events = new HashMap<String, Object>();
		this.comments = new HashMap<String, Map<String, Object>>();
		this.position = new HashMap<String, Object>();
		
		this.colorByValue = new ArrayList<ColorByValueRule>();
		this.grf = new GenRowFilters();
		this.orderBys = new ArrayList<QueryColumnOrderBySelector>();
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
			return new HashMap<String, Object>();
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
					return new HashMap<String, Object>();
				}
			} else {
				// well, can't find this...
				// just return empty
				return new HashMap<String, Object>();
			}
		}
		
		return innerObj;
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
						mainMap.putAll(newMap);
					}
				} else {
					// brand new key
					// put all into the main map
					mainMap.putAll(newMap);
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
	
	public void setPanelView(String view) {
		this.view = view;
		// set the current view options
		Map<String, String> thisViewMap = this.viewOptionsMap.get(view);
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
	public void appendPanelViewOptions(String view, Map<String, String> viewOptions) {
		if(viewOptions != null && !viewOptions.isEmpty()) {
			// view options is append only
			if(this.viewOptionsMap.containsKey(view)) {
				Map<String, String> thisViewMap = this.viewOptionsMap.get(view);
				thisViewMap.putAll(viewOptions);
			} else {
				this.viewOptionsMap.put(view, viewOptions);
			}
		}
		
		Map<String, String> thisViewMap = this.viewOptionsMap.get(view);
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
	
	public Map<String, Map<String, String>> getPanelViewOptions() {
		return this.viewOptionsMap;
	}
	
	public void setPanelViewOptions(Map<String, Map<String, String>> viewOptions) {
		if(viewOptions != null) {
			this.viewOptionsMap = viewOptions;
		}
		
		// set the current view options
		Map<String, String> thisViewMap = this.viewOptionsMap.get(view);
		if(thisViewMap != null) {
			this.viewOptions = GSON.toJson(thisViewMap);
		}
	}
	
	public void setPosition(Map<String, Object> position) {
		this.position = position;
	}
	
	public Map<String, Object> getPosition() {
		return this.position;
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
	
	/**
	 * Return the panel level sorts
	 * @return
	 */
	public List<QueryColumnOrderBySelector> getPanelOrderBys() {
		return this.orderBys;
	}
	
	public void setPanelOrderBys(List<QueryColumnOrderBySelector> orderBys) {
		this.orderBys = orderBys;
	}
	
	/**
	 * Take all the properties of another insight panel
	 * and set them for this panel
	 * @param existingPanel
	 */
	public void clone(InsightPanel existingPanel) {
		Gson gson =  new GsonBuilder()
				.disableHtmlEscaping()
				.excludeFieldsWithModifiers(Modifier.STATIC)
				.registerTypeAdapter(Double.class, new NumberAdapter())
				.registerTypeAdapter(ColorByValueRule.class, new ColorByValueRuleAdapter())
				.create();
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
			this.addColorByValue(gson.fromJson(gson.toJson(rule), ColorByValueRule.class));
		}
	}

}
