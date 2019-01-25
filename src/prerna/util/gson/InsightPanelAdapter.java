package prerna.util.gson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.om.ColorByValueRule;
import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;

public class InsightPanelAdapter extends TypeAdapter<InsightPanel> {
	
	private static final Gson GSON = GsonUtility.getDefaultGson();
	private static final Gson SIMPLE_GSON = new Gson();

	private boolean simple = false;
	
	public InsightPanelAdapter() {
		
	}
	
	public InsightPanelAdapter(boolean simple) {
		this.simple = simple;
	}
	
	public void setSimple(boolean simple) {
		this.simple= simple;
	}
	
	@Override
	public void write(JsonWriter out, InsightPanel value) throws IOException {
		out.beginObject();

		out.name("panelId").value(value.getPanelId());
		out.name("panelLabel").value(value.getPanelLabel());
		out.name("view").value(value.getPanelView());
		out.name("viewOptions").value(value.getPanelActiveViewOptions());
		out.name("viewOptionsMap");
		{
			Map<String, Map<String, String>> obj = value.getPanelViewOptions();
			TypeAdapter adapter = SIMPLE_GSON.getAdapter(obj.getClass());
			adapter.write(out, obj);
		}
		out.name("config");
		{
			Map<String, Object> obj = value.getConfig();
			TypeAdapter adapter = SIMPLE_GSON.getAdapter(obj.getClass());
			adapter.write(out, obj);
		}
		out.name("ornaments");
		{
			Map<String, Object> obj = value.getOrnaments();
			TypeAdapter adapter = SIMPLE_GSON.getAdapter(obj.getClass());
			adapter.write(out, obj);
		}
		out.name("events");
		{
			Map<String, Object> obj = value.getEvents();
			TypeAdapter adapter = SIMPLE_GSON.getAdapter(obj.getClass());
			adapter.write(out, obj);
		}
		out.name("comments");
		{
			Map<String, Map<String, Object>> obj = value.getComments();
			TypeAdapter adapter = SIMPLE_GSON.getAdapter(obj.getClass());
			adapter.write(out, obj);
		}
		out.name("position");
		{
			Map<String, Object> obj = value.getPosition();
			TypeAdapter adapter = SIMPLE_GSON.getAdapter(obj.getClass());
			adapter.write(out, obj);
		}
		out.name("filters");
		// this adapter will write an array
		GenRowFiltersAdapter grsAdapter = new GenRowFiltersAdapter();
		grsAdapter.write(out, value.getPanelFilters());

		out.name("order");
		out.beginArray();
		List<QueryColumnOrderBySelector> orders = value.getPanelOrderBys();
		for(int i = 0; i < orders.size(); i++) {
			out.value(GSON.toJson(orders.get(i)));
		}
		out.endArray();
		
		out.name("cbv");
		out.beginArray();
		List<ColorByValueRule> cbvList = value.getColorByValue();
		for(ColorByValueRule rule : cbvList) {
			ColorByValueRuleAdapter cbvAdapter = new ColorByValueRuleAdapter(simple);
			cbvAdapter.write(out, rule);
		}
		out.endArray();
		
		out.endObject();
	}
	
	@Override
	public InsightPanel read(JsonReader in) throws IOException {
		String panelId = null;
		String panelLabel = null;
		String view = null;
		String viewOptions = null;
		Map<String, Map<String, String>> viewOptionsMap = null;
		Map<String, Object> config = null;
		Map<String, Object> ornaments = null;
		Map<String, Object> events = null;
		GenRowFilters grf = null;
		List<QueryColumnOrderBySelector> orders = null;
		Map<String, Map<String, Object>> comments = null;
		Map<String, Object> position = null;
		List<ColorByValueRule> cbvList = null;
		
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			JsonToken peak = in.peek();
			if(peak == JsonToken.NULL) {
				in.nextNull();
				continue;
			}
			
			// majority of things are just strings
			String value = null;
			if(in.peek() == JsonToken.STRING) {
				value = in.nextString();
			}
			
			if(key.equals("panelId")) {
				panelId = value;
			} else if(key.equals("panelLabel")) {
				panelLabel = value;
			} else if(key.equals("view")) {
				view = value;
			} else if(key.equals("viewOptions")) {
				viewOptions = value;
			} else if(key.equals("viewOptionsMap")) {
				TypeAdapter adapter = SIMPLE_GSON.getAdapter(Map.class);
				viewOptionsMap = (Map<String, Map<String, String>>) adapter.read(in);
				
			} else if(key.equals("config")) {
				TypeAdapter adapter = SIMPLE_GSON.getAdapter(Map.class);
				config = (Map<String, Object>) adapter.read(in);
			
			} else if(key.equals("ornaments")) {
				TypeAdapter adapter = SIMPLE_GSON.getAdapter(Map.class);
				ornaments = (Map<String, Object>) adapter.read(in);
			
			} else if(key.equals("events")) {
				TypeAdapter adapter = SIMPLE_GSON.getAdapter(Map.class);
				events = (Map<String, Object>) adapter.read(in);
			
			} else if(key.equals("comments")) {
				TypeAdapter adapter = SIMPLE_GSON.getAdapter(Map.class);
				comments = (Map<String, Map<String, Object>>) adapter.read(in);
			
			} else if(key.equals("position")) {
				TypeAdapter adapter = SIMPLE_GSON.getAdapter(Map.class);
				position = (Map<String, Object>) adapter.read(in);
			} 
			
			// the values that are not strings
			else if(key.equals("filters")) {
				GenRowFiltersAdapter adapter = new GenRowFiltersAdapter();
				grf = adapter.read(in);
				
			} else if(key.equals("order")) {
				in.beginArray();
				orders = new Vector<QueryColumnOrderBySelector>();
				while(in.hasNext()) {
					String str = in.nextString();
					QueryColumnOrderBySelector s = GSON.fromJson(str, QueryColumnOrderBySelector.class);
					orders.add(s);
				}
				in.endArray();
			
			} else if(key.equals("cbv")) {
				cbvList = new ArrayList<ColorByValueRule>();
				in.beginArray();
				while(in.hasNext()) {
					ColorByValueRuleAdapter cbvAdapter = new ColorByValueRuleAdapter();
					ColorByValueRule cbvRule = cbvAdapter.read(in);
					cbvList.add(cbvRule);
				}
				in.endArray();
			}
		}
		in.endObject();

		InsightPanel panel = new InsightPanel(panelId);
		panel.setPanelLabel(panelLabel);
		panel.setPanelView(view);
		panel.setPanelActiveViewOptions(viewOptions);
		panel.setPanelViewOptions(viewOptionsMap);
		panel.addConfig(config);
		panel.addOrnaments(ornaments);
		panel.addEvents(events);
		panel.addPanelFilters(grf);
		panel.setPanelOrderBys(orders);
		panel.setComments(comments);
		panel.setPosition(position);
		if(cbvList != null) {
			panel.getColorByValue().addAll(cbvList);
		}
		return panel;
	}

}
