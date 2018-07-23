package prerna.util.gson;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;

public class InsightPanelAdapter extends TypeAdapter<InsightPanel> {
	
	
//	private String panelId;
//	// label for the panel
//	private String panelLabel;
//	// current UI view for the panel
//	private transient String view;
//	// view options on the current view
//	private transient String viewOptions;
//	// state held for UI options on the panel
//	private transient Map<String, Object> ornaments;
//	// state held for events on the panel
//	private transient Map<String, Object> events;
//	// set of filters that are only applied to this panel
//	private transient GenRowFilters grf;
//	// set the sorts on the panel
//	private transient List<QueryColumnOrderBySelector> orderBys;
//	// list of comments added to the panel
//	// key is the id pointing to the info on the comment
//	// the info on the comment also contains the id
//	private transient Map<String, Map<String, Object>> comments;
//	// map to store the panel position
//	private transient Map<String, Object> position;
	
	private static final Gson GSON = GsonUtility.getDefaultGson();
	
	@Override
	public void write(JsonWriter out, InsightPanel value) throws IOException {
		out.beginObject();

		out.name("panelId").value(value.getPanelId());
		out.name("panelLabel").value(value.getPanelLabel());
		out.name("view").value(value.getPanelView());
		out.name("viewOptions").value(value.getPanelViewOptions());
		out.name("ornaments").value(GSON.toJson(value.getOrnaments()));
		out.name("events").value(GSON.toJson(value.getEvents()));
		out.name("filters").value(GSON.toJson(value.getPanelFilters()));
		out.name("order").value(GSON.toJson(value.getPanelOrderBys()));
		out.name("comments").value(GSON.toJson(value.getComments()));
		out.name("position").value(GSON.toJson(value.getPosition()));

		out.endObject();
	}
	@Override
	public InsightPanel read(JsonReader in) throws IOException {

		String panelId;
		String panelLabel;
		String view;
		String viewOptions;
		Map<String, Object> ornaments;
		Map<String, Object> events;
		GenRowFilters grf;
		List<QueryColumnOrderBySelector> orderBys;
		Map<String, Map<String, Object>> comments;
		Map<String, Object> position;
		
		
		
		
		return null;
	}

}
