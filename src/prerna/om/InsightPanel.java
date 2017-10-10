package prerna.om;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import prerna.query.querystruct.GenRowFilters;
import prerna.query.querystruct.QueryColumnOrderBySelector;

public class InsightPanel {

	// unique id for the panel
	private String panelId;
	// label for the panel
	private String panelLabel;
	// current UI view for the panel
	private transient String view;
	// view options on the current view
	private transient String viewOptions;
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
	
	public InsightPanel(String panelId) {
		this.panelId = panelId;
		this.panelLabel = panelId;
		this.grf = new GenRowFilters();
		this.orderBys = new ArrayList<QueryColumnOrderBySelector>();
		this.ornaments = new HashMap<String, Object>();
		this.events = new HashMap<String, Object>();
		this.comments = new HashMap<String, Map<String, Object>>();
		this.position = new HashMap<String, Object>();
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
		this.view = view;;
	}

	public String getPanelView() {
		return this.view;
	}
	
	public void setPanelViewOptions(String viewOptions) {
		this.viewOptions = viewOptions;
	}
	
	public String getPanelViewOptions() {
		return this.viewOptions;
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
		//TODO: make sure I update this every time i add a new field!!!
		//TODO: make sure I update this every time i add a new field!!!
		//TODO: make sure I update this every time i add a new field!!!
		//TODO: make sure I update this every time i add a new field!!!
		Gson gson = new Gson();
		this.ornaments.putAll(gson.fromJson(gson.toJson(existingPanel.ornaments), Map.class));
		this.comments.putAll(gson.fromJson(gson.toJson(existingPanel.comments), Map.class));
		this.events.putAll(gson.fromJson(gson.toJson(existingPanel.events), Map.class));
		this.grf = existingPanel.grf.copy();
		this.view = existingPanel.view;
		this.panelLabel = existingPanel.panelLabel + " Clone";
	}
}
