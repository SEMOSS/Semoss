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

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.qs.SubQueryExpression;

public class SubQueryExpressionAdapter extends AbstractSemossTypeAdapter<SubQueryExpression> {

	@Override
	public SubQueryExpression read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		SubQueryExpression value = new SubQueryExpression();
		SelectQueryStruct qs = null;
		AbstractQueryStruct.QUERY_STRUCT_TYPE qsType = null;
		in.beginObject();
		in.nextName();
		if (in.peek() == JsonToken.NULL) {
			// we have an empty object
			in.nextNull();
		} else {
			qsType = AbstractQueryStruct.QUERY_STRUCT_TYPE.valueOf(in.nextString());
		}
		in.nextName();
		if (in.peek() == JsonToken.NULL) {
			// we have an empty object
			in.nextNull();
		} else {
			AbstractSemossTypeAdapter adapter = AbstractQueryStruct.getAdapterForQueryStruct(qsType);
			adapter.setInsight(this.insight);
			qs = (SelectQueryStruct) adapter.read(in);
		}
		in.endObject();

		value.setQs(qs);
		value.setInsight(this.insight);
		return value;
	}

	@Override
	public void write(JsonWriter out, SubQueryExpression value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		out.beginObject();
		// write the QS
		if (value.getQs() == null) {
			out.name("qsType");
			out.nullValue();
			out.name("qs");
			out.nullValue();
		} else {
			SelectQueryStruct qs = value.getQs();
			out.name("qsType");
			out.value(qs.getQsType() + "");
			out.name("qs");
			TypeAdapter adapter = AbstractQueryStruct.getAdapterForQueryStruct(qs.getQsType());
			adapter.write(out, qs);
		}
		out.endObject();
	}

}
