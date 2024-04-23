package prerna.util.gson;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class ExcelQueryStructAdapter  extends AbstractSemossTypeAdapter<ExcelQueryStruct> {

	@Override
	public ExcelQueryStruct read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		ExcelQueryStruct qs = new ExcelQueryStruct();

		in.beginObject();
		while(in.hasNext()) {
			String name = in.nextName();
			if(in.peek() == JsonToken.NULL) {
				in.nextNull();
				continue;
			}
			
			if(name.equals("qsType")) {
				qs.setQsType(QUERY_STRUCT_TYPE.valueOf(in.nextString()));
			} else if(name.equals("isDistinct")) {
				qs.setDistinct(in.nextBoolean());
			} else if(name.equals("overrideImplicit")) {
				qs.setDistinct(in.nextBoolean());
			} else if(name.equals("limit")) {
				qs.setLimit(in.nextLong());
			} else if(name.equals("offset")) {
				qs.setOffSet(in.nextLong());
			} else if(name.equals("queryAll")) {
				qs.setQueryAll(in.nextBoolean());
			}
			
			// excel stuff
			else if(name.equals("sheetName")) {
				qs.setSheetName(in.nextString());
			} else if(name.equals("sheetRange")) {
				qs.setSheetRange(in.nextString());
			}  else if(name.equals("excelPassword")) {
				qs.setPassword(in.nextString());
			}
			
			// all files
			else if(name.equals("filePath")) {
				qs.setFilePath(in.nextString());
			} else if(name.equals("newHeaderNames")) {
				Map<String, String> newHeaderNames = IQuerySelectorAdapterHelper.readStringMap(in);
				qs.setNewHeaderNames(newHeaderNames);
			} else if(name.equals("columnTypes")) {
				Map<String, String> columnTypes = IQuerySelectorAdapterHelper.readStringMap(in);
				qs.setColumnTypes(columnTypes);
			} else if(name.equals("additionalTypes")) {
				Map<String, String> additionalTypes = IQuerySelectorAdapterHelper.readStringMap(in);
				qs.setAdditionalTypes(additionalTypes);
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
				qs.setExplicitFilters(readGrf(in));
			}
			// explicit filters
			else if(name.equals("implicitFilters")) {
				qs.setImplicitFilters(readGrf(in));
			}
			// explicit filters
			else if(name.equals("havingFilters")) {
				qs.setHavingFilters(readGrf(in));
			}
			// group bys
			else if(name.equals("groups")) {
				in.beginArray();
				List<IQuerySelector> groupBy = new Vector<>();
				while(in.hasNext()) {
					IQuerySelectorAdapter selectorAdapter = new IQuerySelectorAdapter();
					IQuerySelector selector = selectorAdapter.read(in);
					groupBy.add((QueryColumnSelector) selector);
				}
				in.endArray();
				qs.setGroupBy(groupBy);
			}
			// orders
			else if(name.equals("orders")) {
				List<IQuerySort> orders = new Vector<IQuerySort>();
				in.beginArray();
				orders = new Vector<IQuerySort>();
				while(in.hasNext()) {
					IQuerySortAdapter sortAdapter = new IQuerySortAdapter();
					IQuerySort orderBy = sortAdapter.read(in);
					orders.add(orderBy);
				}
				in.endArray();
				qs.setOrderBy(orders);
			}
			else if(name.equals("relations")) {
				Set<IRelation> relations = new LinkedHashSet<>();
				in.beginArray();
				while(in.hasNext()) {
					IRelationAdapter relationAdapter = new IRelationAdapter();
					relationAdapter.setInsight(this.insight);
					IRelation relation = relationAdapter.read(in);
					relations.add(relation);
				}
				in.endArray();
				qs.setRelations(relations);
			}
		}
		in.endObject();

		return qs;
	}
	
	/**
	 * Used to read the grf
	 * @param in
	 * @return
	 * @throws IOException
	 */
	private GenRowFilters readGrf(JsonReader in) throws IOException {
		GenRowFiltersAdapter adapter = new GenRowFiltersAdapter();
		GenRowFilters grf = adapter.read(in);
		if(grf == null) {
			return new GenRowFilters();
		}
		return grf;
	}

	@Override
	public void write(JsonWriter out, ExcelQueryStruct value) throws IOException {
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
		out.name("isDistinct").value(value.isDistinct());
		out.name("overrideImplicit").value(value.isOverrideImplicit());
		out.name("limit").value(value.getLimit());
		out.name("offset").value(value.getOffset());
		out.name("queryAll").value(value.getQueryAll());
		
		// excel stuff
		out.name("sheetName").value(value.getSheetName());
		out.name("sheetRange").value(value.getSheetRange());
		out.name("excelPassword").value(value.getPassword());
		// all files
		out.name("filePath").value(value.getFilePath());
		out.name("newHeaderNames");
		IQuerySelectorAdapterHelper.writeStringMap(out, value.getNewHeaderNames());
		out.name("columnTypes");
		IQuerySelectorAdapterHelper.writeStringMap(out, value.getColumnTypes());
		out.name("additionalTypes");
		IQuerySelectorAdapterHelper.writeStringMap(out, value.getAdditionalTypes());

		// now the fun stuff
		
		// selectors
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
		
		// filters
		GenRowFilters explicitFilters = value.getExplicitFilters();
		int numExplicitFilters = explicitFilters.size();
		if(numExplicitFilters > 0) {
			out.name("explicitFilters");
			writeGrf(out, explicitFilters);
		}

		GenRowFilters implicitFilters = value.getImplicitFilters();
		int numImplicitFilters = implicitFilters.size();
		if(numImplicitFilters > 0) {
			out.name("implicitFilters");
			writeGrf(out, implicitFilters);
		}

		GenRowFilters havingFilters = value.getHavingFilters();
		int numHavingFilters = havingFilters.size();
		if(numHavingFilters > 0) {
			out.name("havingFilters");
			writeGrf(out, havingFilters);
		}

		// groups
		List<IQuerySelector> groups = value.getGroupBy();
		int numGroups = groups.size();
		if(numGroups > 0) {
			out.name("groups");
			out.beginArray();
			for(int i = 0; i < numGroups; i++) {
				IQuerySelector s = groups.get(i);
				TypeAdapter adapter = IQuerySelector.getAdapterForSelector(s.getSelectorType());
				adapter.write(out, s);
			}
			out.endArray();
		}

		// orders
		List<IQuerySort> orders = value.getOrderBy();
		int numOrders = orders.size();
		if(numOrders > 0) {
			out.name("orders");
			out.beginArray();
			for(int i = 0; i < orders.size(); i++) {
				IQuerySort orderBy = orders.get(i);
				TypeAdapter adapter = IQuerySort.getAdapterForSort(orderBy.getQuerySortType());
				adapter.write(out, orderBy);
			}
			out.endArray();
		}
		
		// relationships
		Set<IRelation> relationships = value.getRelations();
		if(relationships != null && !relationships.isEmpty()) {
			out.name("relations");
			out.beginArray();
			for(IRelation rel : relationships) {
				TypeAdapter adapter = IRelation.getAdapterForRelation(rel.getRelationType());
				adapter.write(out, rel);
			}
			out.endArray();
		}

		out.endObject();
	}
	
	/**
	 * To write the grf
	 * @param out
	 * @param grf
	 * @throws IOException
	 */
	private void writeGrf(JsonWriter out, GenRowFilters grf) throws IOException {
		GenRowFiltersAdapter adapter = new GenRowFiltersAdapter();
		adapter.write(out, grf);
	}

}
