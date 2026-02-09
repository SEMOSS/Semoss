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
import java.util.List;
import java.util.Vector;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class QueryFunctionSelectorAdapter extends AbstractSemossTypeAdapter<QueryFunctionSelector> implements IQuerySelectorAdapterHelper {
	
	@Override
	public QueryFunctionSelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		// remove the beginning objects
		in.beginObject();
		in.nextName();
		in.nextString();
		in.nextName();
		
		// now we read the actual content
		QueryFunctionSelector value = readContent(in);
		in.endObject();
		return value;
	}
	
	@Override
	public QueryFunctionSelector readContent(JsonReader in) throws IOException {
		QueryFunctionSelector value = new QueryFunctionSelector();
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			if(key.equals("alias")) {
				value.setAlias(in.nextString());
			} else if(key.equals("distinct")) {
				value.setDistinct(in.nextBoolean());
			} else if(key.equals("colCast")) {
				value.setColCast(in.nextString());
			} else if(key.equals("function")) {
				value.setFunction(in.nextString());
			} else if (key.equals("dataType")) {
				value.setDataType(in.nextString());

			} else if(key.equals("innerSelectors")) {
				List<IQuerySelector> innerList = new Vector<IQuerySelector>();
				
				in.beginArray();
				while(in.hasNext()) {
					IQuerySelectorAdapter innerAdapter = new IQuerySelectorAdapter();
					innerAdapter.setInsight(this.insight);
					IQuerySelector innerSelector = innerAdapter.read(in);
					innerList.add(innerSelector);
				}
				in.endArray();
				
				value.setInnerSelector(innerList);
			} else if(key.equals("additionalFunctionParams")) {
				List<Object[]> additionalParams = new Vector<Object[]>();
				
				in.beginArray();
				while(in.hasNext()) {
					in.beginArray();
					Vector<Object> paramArray = new Vector<Object>();
					while(in.hasNext()) {
						if(in.peek() == JsonToken.STRING) {
							paramArray.add(in.nextString());
						} else if(in.peek() == JsonToken.NUMBER) {
							paramArray.add(in.nextDouble());
						} else if(in.peek() == JsonToken.BOOLEAN) {
							paramArray.add(in.nextBoolean());
						} else if(in.peek() == JsonToken.NULL) {
							in.nextNull();
							paramArray.add(null);
						}
					}
					in.endArray();
					additionalParams.add(paramArray.toArray());
				}
				in.endArray();
				
				value.setAdditionalFunctionParams(additionalParams);
			}
		}
		in.endObject();
		return value;
	}


	@Override
	public void write(JsonWriter out, QueryFunctionSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		// always start with the type of the query selector
		out.beginObject();
		out.name("type").value(IQuerySelector.SELECTOR_TYPE.FUNCTION.toString());
		out.name("content");

		// content object
		out.beginObject();
		out.name("alias").value(value.getAlias());
		out.name("function").value(value.getFunction());
		out.name("distinct").value(value.isDistinct());
		out.name("colCast").value(value.getColCast());
		out.name("dataType").value(value.getDataType());

		out.name("innerSelectors");
		out.beginArray();
		List<IQuerySelector> innerList =  value.getInnerSelector();
		for(IQuerySelector inner : innerList) {
			TypeAdapter leftOutput = IQuerySelector.getAdapterForSelector(inner.getSelectorType());
			leftOutput.write(out, inner);
		}
		out.endArray();
		
		out.name("additionalFunctionParams");
		out.beginArray();
		List<Object[]> additionalParams = value.getAdditionalFunctionParams();
		if(additionalParams != null) {
			for(Object[] paramArray : additionalParams) {
				out.beginArray();
				if(paramArray != null) {
					for(Object param : paramArray) {
						if(param == null) {
							out.nullValue();
						} else if(param instanceof String) {
							out.value((String) param);
						} else if(param instanceof Number) {
							out.value((Number) param);
						} else if(param instanceof Boolean) {
							out.value((Boolean) param);
						} else {
							out.value(param.toString());
						}
					}
				}
				out.endArray();
			}
		}
		out.endArray();
		
		out.endObject();
		
		out.endObject();
	}
}