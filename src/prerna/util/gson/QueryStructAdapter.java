package prerna.util.gson;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class QueryStructAdapter  extends TypeAdapter<SelectQueryStruct> {

	private static final Gson SIMPLE_GSON = new Gson();

	@Override
	public SelectQueryStruct read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		SelectQueryStruct qs = new SelectQueryStruct();

		in.beginObject();
		while(in.hasNext()) {
			String name = in.nextName();
			if(name.equals("qsType")) {
				qs.setQsType(QUERY_STRUCT_TYPE.valueOf(in.nextString()));
			} else if(name.equals("engineName")) {
				qs.setEngineName(in.nextString());
			} else if(name.equals("isDistinct")) {
				qs.setDistinct(in.nextBoolean());
			} else if(name.equals("overrideImplicit")) {
				qs.setDistinct(in.nextBoolean());
			} else if(name.equals("limit")) {
				qs.setLimit(in.nextLong());
			} else if(name.equals("offset")) {
				qs.setOffSet(in.nextLong());
			} else if(name.equals("relations")) {
				String str = in.nextString();
				Map<String, Map<String, List>> relations = SIMPLE_GSON.fromJson(str, new TypeToken<Map<String, Map<String, List>>>() {}.getType());
				qs.setRelations(relations);
			}
			// group bys
			else if(name.equals("groups")) {
				in.beginArray();
				List<QueryColumnSelector> groupBy = new Vector<QueryColumnSelector>();
				while(in.hasNext()) {
					String str = in.nextString();
					QueryColumnSelector s = SIMPLE_GSON.fromJson(str, QueryColumnSelector.class);
					groupBy.add(s);
				}
				in.endArray();
				qs.setGroupBy(groupBy);
			}
			// orders
			else if(name.equals("orders")) {
				in.beginArray();
				List<QueryColumnOrderBySelector> orders = new Vector<QueryColumnOrderBySelector>();
				while(in.hasNext()) {
					String str = in.nextString();
					QueryColumnOrderBySelector s = SIMPLE_GSON.fromJson(str, QueryColumnOrderBySelector.class);
					orders.add(s);
				}
				in.endArray();
				qs.setOrderBy(orders);
			}
			// selectors
			else if(name.equals("selectors")) {
				in.beginArray();
				List<IQuerySelector> selectors = new Vector<IQuerySelector>();
				while(in.hasNext()) {
					IQuerySelectorAdapter selectorAdapter = new IQuerySelectorAdapter();
					IQuerySelector selector = selectorAdapter.read(in);
					selectors.add(selector);
				}
				in.endArray();
				qs.setSelectors(selectors);
			}
			// explicit filters
			else if(name.equals("explicitFilters")) {
				in.beginArray();
				GenRowFilters grf = new GenRowFilters();
				while(in.hasNext()) {
					IQueryFilterAdapter filterAdapter = new IQueryFilterAdapter();
					IQueryFilter filter = filterAdapter.read(in);
					grf.addFilters(filter);
				}
				in.endArray();
				qs.setExplicitFilters(grf);
			}
			// explicit filters
			else if(name.equals("implicitFilters")) {
				in.beginArray();
				GenRowFilters grf = new GenRowFilters();
				while(in.hasNext()) {
					IQueryFilterAdapter filterAdapter = new IQueryFilterAdapter();
					IQueryFilter filter = filterAdapter.read(in);
					grf.addFilters(filter);
				}
				in.endArray();
				qs.setImplicitFilters(grf);
			}
			// explicit filters
			else if(name.equals("havingFilters")) {
				in.beginArray();
				GenRowFilters grf = new GenRowFilters();
				while(in.hasNext()) {
					IQueryFilterAdapter filterAdapter = new IQueryFilterAdapter();
					IQueryFilter filter = filterAdapter.read(in);
					grf.addFilters(filter);
				}
				in.endArray();
				qs.setHavingFilters(grf);
			}

		}
		in.endObject();

		return qs;
	}

	@Override
	public void write(JsonWriter out, SelectQueryStruct value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		// this will be fun...
		// will try to go ahead and write everything

		out.beginObject();

		// lets do the easy ones first
		// qs type
		out.name("qsType").value(value.getQsType().toString());
		if(value.getEngineName() != null) {
			out.name("engineName").value(value.getEngineName());
		}
		out.name("isDistinct").value(value.isDistinct());
		out.name("overrideImplicit").value(value.isOverrideImplicit());
		out.name("limit").value(value.getLimit());
		out.name("offset").value(value.getOffset());
		out.name("relations").value(SIMPLE_GSON.toJson(value.getRelations()));

		// now the fun stuff

		List<QueryColumnSelector> groups = value.getGroupBy();
		int numGroups = groups.size();
		if(numGroups > 0) {
			out.name("groups");
			out.beginArray();
			for(int i = 0; i < numGroups; i++) {
				out.value(SIMPLE_GSON.toJson(groups.get(i)));
			}
			out.endArray();
		}

		List<QueryColumnOrderBySelector> orders = value.getOrderBy();
		int numOrders = orders.size();
		if(numOrders > 0) {
			out.name("orders");
			out.beginArray();
			for(int i = 0; i < numOrders; i++) {
				out.value(SIMPLE_GSON.toJson(orders.get(i)));
			}
			out.endArray();
		}

		List<IQuerySelector> selectors = value.getSelectors();
		int numSelectors = selectors.size();
		if(numSelectors > 0) {
			out.name("selectors");
			out.beginArray();
			for(int i = 0; i < numSelectors; i++) {
				IQuerySelector s = selectors.get(i);
				TypeAdapter adapter = IQuerySelector.getAdapterForSelector(s.getSelectorType());
				adapter.write(out, s);
			}
			out.endArray();
		}

		GenRowFilters explicitFilters = value.getExplicitFilters();
		int numExplicitFilters = explicitFilters.size();
		if(numExplicitFilters > 0) {
			out.name("explicitFilters");
			out.beginArray();
			List<IQueryFilter> filters = explicitFilters.getFilters();
			for(int i = 0; i < numExplicitFilters; i++) {
				IQueryFilter f = filters.get(i);
				TypeAdapter adapter = IQueryFilter.getAdapterForFilter(f.getQueryFilterType());
				adapter.write(out, f);
			}
			out.endArray();
		}

		GenRowFilters implicitFilters = value.getImplicitFilters();
		int numImplicitFilters = implicitFilters.size();
		if(numImplicitFilters > 0) {
			out.name("implicitFilters");
			out.beginArray();
			List<IQueryFilter> filters = implicitFilters.getFilters();
			for(int i = 0; i < numImplicitFilters; i++) {
				IQueryFilter f = filters.get(i);
				TypeAdapter adapter = IQueryFilter.getAdapterForFilter(f.getQueryFilterType());
				adapter.write(out, f);
			}
			out.endArray();
		}

		GenRowFilters havingFilters = value.getHavingFilters();
		int numHavingFilters = havingFilters.size();
		if(numHavingFilters > 0) {
			out.name("havingFilters");
			out.beginArray();
			List<IQueryFilter> filters = havingFilters.getFilters();
			for(int i = 0; i < numHavingFilters; i++) {
				IQueryFilter f = filters.get(i);
				TypeAdapter adapter = IQueryFilter.getAdapterForFilter(f.getQueryFilterType());
				adapter.write(out, f);
			}
			out.endArray();
		}

		out.endObject();
	}

}
