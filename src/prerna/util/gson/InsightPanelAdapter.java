package prerna.util.gson;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;

public class InsightPanelAdapter extends TypeAdapter<InsightPanel> {
	
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

		String panelId = null;
		String panelLabel = null;
		String view = null;
		String viewOptions = null;
		Map<String, Object> ornaments = null;
		Map<String, Object> events = null;
		GenRowFilters grf = null;
		List<QueryColumnOrderBySelector> orderBys = null;
		Map<String, Map<String, Object>> comments = null;
		Map<String, Object> position = null;
		
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			JsonToken peak = in.peek();
			if(peak == JsonToken.NULL) {
				in.nextNull();
				continue;
			}
			String value = in.nextString();
			if(key.equals("panelId")) {
				panelId = value;
			} else if(key.equals("panelLabel")) {
				panelLabel = value;
			} else if(key.equals("view")) {
				view = value;
			} else if(key.equals("viewOptions")) {
				viewOptions = value;
			} else if(key.equals("ornaments")) {
				ornaments = GSON.fromJson(value, Map.class);
			} else if(key.equals("events")) {
				events = GSON.fromJson(value, Map.class);
			} else if(key.equals("fitlers")) {
				grf = GSON.fromJson(value, GenRowFilters.class);
			} else if(key.equals("order")) {
				orderBys = GSON.fromJson(value, List.class);
			} else if(key.equals("comments")) {
				comments = GSON.fromJson(value, Map.class);
			} else if(key.equals("position")) {
				position = GSON.fromJson(value, Map.class);
			}
		}
		in.endObject();

		InsightPanel panel = new InsightPanel(panelId);
		panel.setPanelLabel(panelLabel);
		panel.setPanelView(view);
		panel.setPanelViewOptions(viewOptions);
		panel.addOrnaments(ornaments);
		panel.addEvents(events);
		panel.addPanelFilters(grf);
		panel.setPanelOrderBys(orderBys);
		panel.setComments(comments);
		panel.setPosition(position);
		return panel;
	}

}
