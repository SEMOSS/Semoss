package prerna.util.gson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.date.SemossDate;
import prerna.om.ColorByValueRule;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.reactor.export.IFormatter;
import prerna.sablecc2.om.task.options.TaskOptions;

public class InsightPanelAdapter extends AbstractSemossTypeAdapter<InsightPanel> {
	
	private static Gson GSON = GsonUtility.getDefaultGson();
	
	private static final Gson SIMPLE_GSON =  new GsonBuilder()
			.disableHtmlEscaping()
			.registerTypeAdapter(Double.class, new NumberAdapter())
			.registerTypeAdapter(SemossDate.class, new SemossDateAdapter())
			.create();
	
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
	public InsightPanel read(JsonReader in) throws IOException {
		String panelId = null;
		String sheetId = null;
		String panelLabel = null;
		String view = null;
		String viewOptions = null;
		String renderedViewOptions = null;
		List<Object> dynamicVars = new ArrayList<>();;
		
		Map<String, Map<String, Object>> viewOptionsMap = null;
		Map<String, Object> config = null;
		Map<String, Object> ornaments = null;
		Map<String, Object> events = null;
		GenRowFilters grf = null;
		List<IQuerySort> orders = null;
		Map<String, Map<String, Object>> comments = null;
		List<ColorByValueRule> cbvList = null;
		// additional values that are not always serialized
		SelectQueryStruct lastQs = null;
		Map<String, SelectQueryStruct> layerQueryStructMap = null;
		TaskOptions lastTaskOptions = null;
		Map<String, TaskOptions> layerTaskOptionsMap = null;
		IFormatter lastFormatter = null;
		Map<String, IFormatter> layerFormatterMap = null;

		Integer numCollect = null;
		
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			if(in.peek() == JsonToken.NULL) {
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
			} else if(key.equals("sheetId")) {
				sheetId = value;
			} else if(key.equals("view")) {
				view = value;
			} else if(key.equals("viewOptions")) {
				viewOptions = value;
			} else if(key.equals("renderedViewOptions")) {
				renderedViewOptions = value;
			} else if(key.equals("numCollect")) {
				numCollect = in.nextInt();
			} else if(key.equals("dynamicVars")) {
				in.beginArray();
				while(in.hasNext()) {
					String var = in.nextString();
					dynamicVars.add(var);
				}
				in.endArray();
			} else if(key.equals("viewOptionsMap")) {
				TypeAdapter adapter = SIMPLE_GSON.getAdapter(Map.class);
				viewOptionsMap = (Map<String, Map<String, Object>>) adapter.read(in);
				
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
			
			} 
			// the values that are not strings
			else if(key.equals("filters")) {
				GenRowFiltersAdapter adapter = new GenRowFiltersAdapter();
				grf = adapter.read(in);
				
			} else if(key.equals("order")) {
				orders = new Vector<IQuerySort>();
				in.beginArray();
				while(in.hasNext()) {
					IQuerySortAdapter sortAdapter = new IQuerySortAdapter();
					IQuerySort orderBy = sortAdapter.read(in);
					orders.add(orderBy);
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
			
			} else if(key.equals("lastQs")) {
				AbstractSemossTypeAdapter<SelectQueryStruct> adapter = (AbstractSemossTypeAdapter<SelectQueryStruct>) GSON.getAdapter(SelectQueryStruct.class);
				adapter.setInsight(this.insight);
				lastQs = adapter.read(in);

			} else if(key.equals("lastQueryStructMap")) {
				layerQueryStructMap = new HashMap<>();
				in.beginObject();
				while(in.hasNext()) {
					String layerName = in.nextName();
					AbstractSemossTypeAdapter<SelectQueryStruct> adapter = (AbstractSemossTypeAdapter<SelectQueryStruct>) GSON.getAdapter(SelectQueryStruct.class);
					adapter.setInsight(this.insight);
					SelectQueryStruct layerQS = adapter.read(in);
					layerQueryStructMap.put(layerName, layerQS);
				}
				in.endObject();

			} else if(key.equals("lastTaskOptions")) {
				TaskOptionsAdapter adapter = new TaskOptionsAdapter();
				lastTaskOptions = adapter.read(in);
				
			} else if(key.equals("lastTaskOptionsMap")) {
				layerTaskOptionsMap = new HashMap<>();
				in.beginObject();
				while(in.hasNext()) {
					String layerName = in.nextName();
					TaskOptionsAdapter adapter = new TaskOptionsAdapter();
					TaskOptions taskOptions = adapter.read(in);
					layerTaskOptionsMap.put(layerName, taskOptions);
				}
				in.endObject();
			} else if(key.equals("lastFormatter")) {
				IFormatterAdapter adapter = new IFormatterAdapter();
				lastFormatter = adapter.read(in);
				
			} else if(key.equals("lastFormatterMap")) {
				layerFormatterMap = new HashMap<>();
				in.beginObject();
				while(in.hasNext()) {
					String layerName = in.nextName();
					IFormatterAdapter adapter = new IFormatterAdapter();
					IFormatter taskOptions = adapter.read(in);
					layerFormatterMap.put(layerName, taskOptions);
				}
				in.endObject();
			}
		}
		in.endObject();

		// to account for legacy
		if(sheetId == null) {
			sheetId = Insight.DEFAULT_SHEET_ID;
		}
		
		InsightPanel panel = new InsightPanel(panelId, sheetId);
		panel.setPanelLabel(panelLabel);
		panel.setPanelView(view);
		panel.setPanelActiveViewOptions(viewOptions);
		panel.setRenderedViewOptions(renderedViewOptions, dynamicVars);
		panel.setPanelViewOptions(viewOptionsMap);
		panel.addConfig(config);
		panel.addOrnaments(ornaments);
		panel.addEvents(events);
		panel.addPanelFilters(grf);
		panel.setPanelOrderBys(orders);
		panel.setComments(comments);
		if(numCollect != null) {
			panel.setNumCollect(numCollect);
		}
		if(cbvList != null) {
			panel.getColorByValue().addAll(cbvList);
		}
		if(lastQs != null) {
			panel.setLastQs(lastQs);
		}
		if(layerQueryStructMap != null) {
			panel.setLayerQueryStruct(layerQueryStructMap);
		}
		if(lastTaskOptions != null) {
			panel.setLastTaskOptions(lastTaskOptions);
		}
		if(layerTaskOptionsMap != null) {
			panel.setLayerTaskOptions(layerTaskOptionsMap);
		}
		if(lastFormatter != null) {
			panel.setLastFormatter(lastFormatter);
		}
		if(layerFormatterMap != null) {
			panel.setLayerFormatter(layerFormatterMap);
		}

		// return the panel
		return panel;
	}

	@Override
	public void write(JsonWriter out, InsightPanel value) throws IOException {
		out.beginObject();

		out.name("panelId").value(value.getPanelId());
		out.name("panelLabel").value(value.getPanelLabel());
		out.name("sheetId").value(value.getSheetId());
		out.name("view").value(value.getPanelView());
		out.name("viewOptions").value(value.getPanelActiveViewOptions());
		out.name("renderedViewOptions").value(value.getRenderedViewOptions());
		out.name("numCollect").value(value.getNumCollect());
		out.name("dynamicVars");
		out.beginArray();
		if(value.getDynamicVars() != null) {
			for(String var : value.getDynamicVars()) {
				out.value(var);
			}
		}
		out.endArray();
		out.name("viewOptionsMap");
		{
			Map<String, Map<String, Object>> obj = value.getPanelViewOptions();
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
		out.name("filters");
		// this adapter will write an array
		GenRowFiltersAdapter grsAdapter = new GenRowFiltersAdapter();
		grsAdapter.write(out, value.getPanelFilters());

		out.name("order");
		out.beginArray();
		List<IQuerySort> orders = value.getPanelOrderBys();
		for(int i = 0; i < orders.size(); i++) {
			IQuerySort orderBy = orders.get(i);
			TypeAdapter sortAdapter = IQuerySort.getAdapterForSort(orderBy.getQuerySortType());
			sortAdapter.write(out, orderBy);
		}
		out.endArray();
		
		out.name("cbv");
		out.beginArray();
		List<ColorByValueRule> cbvList = value.getColorByValue();
		for(ColorByValueRule rule : cbvList) {
			ColorByValueRuleAdapter cbvAdapter = new ColorByValueRuleAdapter();
			cbvAdapter.setSimple(this.simple);
			cbvAdapter.setInsight(this.insight);
			cbvAdapter.write(out, rule);
		}
		out.endArray();
		
		if(!this.simple) {
			// we will output some things stored on the BE
			// including the last QS executed
			// the task options
			// the layer to task options
			// and the layer to QS
			SelectQueryStruct lastQs = value.getLastQs();
			if(lastQs != null) {
				out.name("lastQs");
				AbstractSemossTypeAdapter adapter = (AbstractSemossTypeAdapter) GSON.getAdapter(lastQs.getClass());
				adapter.write(out, lastQs);
			}
			
			Map<String, SelectQueryStruct> layerQueryStructMap = value.getLayerQueryStruct();
			if(layerQueryStructMap != null && !layerQueryStructMap.isEmpty()) {
				out.name("lastQueryStructMap");
				out.beginObject();
				for(String layer : layerQueryStructMap.keySet()) {
					out.name(layer);
					SelectQueryStruct layerQueryStruct = layerQueryStructMap.get(layer);
					AbstractSemossTypeAdapter adapter = (AbstractSemossTypeAdapter) GSON.getAdapter(layerQueryStruct.getClass());
					adapter.write(out, layerQueryStruct);
				}
				out.endObject();
			}
			
			TaskOptions lastTaskOptions = value.getLastTaskOptions();
			if(lastTaskOptions != null) {
				out.name("lastTaskOptions");
				TypeAdapter adapter = GSON.getAdapter(lastTaskOptions.getClass());
				adapter.write(out, lastTaskOptions);
			}

			Map<String, TaskOptions> layerTaskOptionsMap = value.getLayerTaskOption();
			if(layerTaskOptionsMap != null && !layerTaskOptionsMap.isEmpty()) {
				out.name("lastTaskOptionsMap");
				out.beginObject();
				for(String layer : layerTaskOptionsMap.keySet()) {
					out.name(layer);
					TaskOptions layerTaskOptions = layerTaskOptionsMap.get(layer);
					TypeAdapter adapter = GSON.getAdapter(layerTaskOptions.getClass());
					adapter.write(out, layerTaskOptions);
				}
				out.endObject();
			}
			
			IFormatter lastFormatter = value.lastFormatter();
			if(lastFormatter != null) {
				out.name("lastFormatter");
				TypeAdapter adapter = GSON.getAdapter(lastFormatter.getClass());
				adapter.write(out, lastFormatter);
			}
			
			Map<String, IFormatter> formatOptionsMap = value.getLayerFormatter();
			if(formatOptionsMap != null && !formatOptionsMap.isEmpty()) {
				out.name("lastFormatterMap");
				out.beginObject();
				for(String layer : formatOptionsMap.keySet()) {
					out.name(layer);
					IFormatter layerFormatter = formatOptionsMap.get(layer);
					TypeAdapter adapter = GSON.getAdapter(lastFormatter.getClass());
					adapter.write(out, layerFormatter);
				}
				out.endObject();
			}
		}
		
		out.endObject();
	}
}
