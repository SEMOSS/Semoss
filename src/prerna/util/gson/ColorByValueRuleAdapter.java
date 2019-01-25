package prerna.util.gson;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.om.ColorByValueRule;
import prerna.query.querystruct.SelectQueryStruct;

public class ColorByValueRuleAdapter extends TypeAdapter<ColorByValueRule> {
	
	private static final Gson GSON = GsonUtility.getDefaultGson();
	private static final Gson SIMPLE_GSON = new Gson();
	
	private boolean simple = false;
	
	public ColorByValueRuleAdapter() {
		
	}
	
	public ColorByValueRuleAdapter(boolean simple) {
		this.simple = simple;
	}
	
	public void setSimple(boolean simple) {
		this.simple= simple;
	}
	@Override
	public void write(JsonWriter out, ColorByValueRule value) throws IOException {
		out.beginObject();

		// cbv name
		out.name("name").value(value.getId());
		// options
		if(value.getOptions() != null) {
			out.name("options");
			Map<String, Object> options = value.getOptions();
			TypeAdapter oAdapter = SIMPLE_GSON.getAdapter(options.getClass());
			oAdapter.write(out, options);
		}

		SelectQueryStruct qs = value.getQueryStruct();
		if(simple) {
			out.name("filterInfo");
			List<Map<String, Object>> formattedFilters = qs.getExplicitFilters().getFormatedFilters();
			TypeAdapter fAdapter = SIMPLE_GSON.getAdapter(formattedFilters.getClass());
			fAdapter.write(out, formattedFilters);
			out.name("havingInfo");
			List<Map<String, Object>> formattedHavings = qs.getHavingFilters().getFormatedFilters();
			TypeAdapter hAdapter = SIMPLE_GSON.getAdapter(formattedFilters.getClass());
			hAdapter.write(out, formattedHavings);
		} else {
			out.name("qs");
			// write the QS
			SelectQueryStructAdapter adapter = new SelectQueryStructAdapter();
			adapter.write(out, value.getQueryStruct());
		}
		
		out.endObject();
	}

	@Override
	public ColorByValueRule read(JsonReader in) throws IOException {
		String id = null;
		Map<String, Object> options = null;
		SelectQueryStruct qs = null;
		
		in.beginObject();
		while(in.hasNext()) {
			String name = in.nextName();
			if(name.equals("name")) {
				id = in.nextString();
			} else if(name.equals("options")) {
				//TODO: adding null point as options isn't required yet on FE
				if(in.peek() == JsonToken.NULL) {
					in.nextNull();
				} else {
					TypeAdapter adapter = SIMPLE_GSON.getAdapter(Map.class);
					options = (Map<String, Object>) adapter.read(in);
				}
			} else if(name.equals("qs")) {
				SelectQueryStructAdapter qsAdapter = new SelectQueryStructAdapter();
				qs = qsAdapter.read(in);
			}
		}
		in.endObject();
		
		return new ColorByValueRule(id, qs, options);
	}

}
