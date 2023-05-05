package prerna.util.gson;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.util.insight.InsightUtility;

public class SelectQueryStructAdapter  extends AbstractSemossTypeAdapter<SelectQueryStruct> {
	
	private static final Logger logger = LogManager.getLogger(SelectQueryStructAdapter.class.getName());

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
			} 
			// engineName is legacy key - used in pipeline - do not delete
			else if(name.equals("engineName")) {
				qs.setEngineId(in.nextString());
			} 
			else if(name.equals("engineId")) {
				qs.setEngineId(in.nextString());
			} else if(name.equals("frameName")) {
				qs.setFrameName(in.nextString());
			} else if(name.equals("frameType")) {
				qs.setFrameType(in.nextString());
			} else if(name.equals("isDistinct")) {
				qs.setDistinct(in.nextBoolean());
			} else if(name.equals("overrideImplicit")) {
				qs.setOverrideImplicit(in.nextBoolean());
			} else if(name.equals("limit")) {
				qs.setLimit(in.nextLong());
			} else if(name.equals("offset")) {
				qs.setOffSet(in.nextLong());
			} else if(name.equals("queryAll")) {
				qs.setQueryAll(in.nextBoolean());
			}
			
			// custom query stuff
			else if(name.equals("customFrom")) {
				if(in.peek() == JsonToken.NULL) {
					in.nextNull();
				} else {
					qs.setCustomFrom(in.nextString());
				}
			} else if(name.equals("customFromAliasName")) {
				if(in.peek() == JsonToken.NULL) {
					in.nextNull();
				} else {
					qs.setCustomFromAliasName(in.nextString());
				}
			}
			
			// selectors
			else if(name.equals("selectors")) {
				in.beginArray();
				List<IQuerySelector> selectors = new Vector<IQuerySelector>();
				while(in.hasNext()) {
					IQuerySelectorAdapter selectorAdapter = new IQuerySelectorAdapter();
					selectorAdapter.setInsight(this.insight);
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
					selectorAdapter.setInsight(this.insight);
					IQuerySelector selector = selectorAdapter.read(in);
					
					if(selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
						groupBy.add((QueryColumnSelector) selector);
					} else if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
						groupBy.add((QueryFunctionSelector) selector);
					} else {
						String errorMessage = "Error: Cannot group by non QueryColumnSelector and QueryFunctionSelector types yet...";
						logger.error(errorMessage);
						throw new IllegalArgumentException(errorMessage);
					}
						
				}
				in.endArray();
				qs.setGroupBy(groupBy);
			}
			// orders
			else if(name.equals("orders")) {
				List<IQuerySort> orders = new Vector<IQuerySort>();
				in.beginArray();
				while(in.hasNext()) {
					IQuerySortAdapter sortAdapter = new IQuerySortAdapter();
					sortAdapter.setInsight(this.insight);
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
			else if(name.equals("withPanels")) {
				List<String> panelIds = new Vector<>();
				in.beginArray();
				while(in.hasNext()) {
					String pId = in.nextString();
					panelIds.add(pId);
				}
				in.endArray();
				qs.setPanelIdList(panelIds);
			}
		}
		in.endObject();

		// fill in the references into the query struct
		// for frames and panels
		if(this.insight != null) {
			InsightUtility.fillQueryStructReferences(this.insight, qs);
		}
		
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
		adapter.setInsight(this.insight);
		GenRowFilters grf = adapter.read(in);
		if(grf == null) {
			return new GenRowFilters();
		}
		return grf;
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
		if(value.getEngineId() != null) {
			// engineName is legacy key - used in pipeline - do not delete
			out.name("engineName").value(value.getEngineId());
			out.name("engineId").value(value.getEngineId());
		}
		if(value.getFrameName() != null) {
			out.name("frameName").value(value.getFrameName());
			out.name("frameType").value(value.getFrameType());
		}
		out.name("isDistinct").value(value.isDistinct());
		out.name("overrideImplicit").value(value.isOverrideImplicit());
		out.name("limit").value(value.getLimit());
		out.name("offset").value(value.getOffset());
		out.name("queryAll").value(value.getQueryAll());

		// custom query stuff
		out.name("customFrom").value(value.getCustomFrom());
		out.name("customFromAliasName").value(value.getCustomFromAliasName());
		
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
		
		// do the panels
		// we only do the panel ids
		List<String> panelIds = value.getPanelIdList();
		if(panelIds != null && !panelIds.isEmpty()) {
			out.name("withPanels");
			out.beginArray();
			for(String pId : panelIds) {
				out.value(pId);
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
