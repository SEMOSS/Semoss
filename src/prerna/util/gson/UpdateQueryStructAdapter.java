/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util.gson;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.update.UpdateQueryStruct;

public class UpdateQueryStructAdapter extends AbstractSemossTypeAdapter<UpdateQueryStruct> {
	
	private static final Logger classLogger = LogManager.getLogger(UpdateQueryStructAdapter.class);
	
	private static final Gson GSON = GsonUtility.getDefaultGson();

	@Override
	public UpdateQueryStruct read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		UpdateQueryStruct qs = new UpdateQueryStruct();

		in.beginObject();
		while(in.hasNext()) {
			String name = in.nextName();
			if(name.equals("qsType")) {
				qs.setQsType(QUERY_STRUCT_TYPE.valueOf(in.nextString()));
			} else if(name.equals("engineName")) {
				qs.setEngineId(in.nextString());
			} else if(name.equals("frameName")) {
				qs.setFrameName(in.nextString());
			} else if(name.equals("frameType")) {
				qs.setFrameType(in.nextString());
			} else if(name.equals("overrideImplicit")) {
				qs.setOverrideImplicit(in.nextBoolean());
			} 
			// custom to update query 
			else if(name.equals("updateValues")) {
				TypeAdapter adapter = GSON.getAdapter(List.class);
				List<Object> values = (List<Object>) adapter.read(in);
				qs.setValues(values);
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
	public void write(JsonWriter out, UpdateQueryStruct value) throws IOException {
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
			out.name("engineName").value(value.getEngineId());
		}
		if(value.getFrameName() != null) {
			out.name("frameName").value(value.getFrameName());
			out.name("frameType").value(value.getFrameType());
		}
		out.name("overrideImplicit").value(value.isOverrideImplicit());

		// custom to update query 
		{
			List<Object> values = value.getValues();
			out.name("updateValues");
			TypeAdapter adapter = GSON.getAdapter(values.getClass());
			adapter.write(out, values);
		}
		
		// base query struct
		
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
