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

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;

public class HardSelectQueryStructAdapter extends AbstractSemossTypeAdapter<HardSelectQueryStruct> {

	@Override
	public HardSelectQueryStruct read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		HardSelectQueryStruct qs = new HardSelectQueryStruct();

		in.beginObject();
		while (in.hasNext()) {
			String name = in.nextName();
			if (name.equals("qsType")) {
				qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.valueOf(in.nextString()));
			} else if (name.equals("engineName")) {
				qs.setEngineId(in.nextString());
			} else if (name.equals("frameName")) {
				qs.setFrameName(in.nextString());
			} else if (name.equals("frameType")) {
				qs.setFrameType(in.nextString());
			} else if (name.equals("query")) {
				qs.setQuery(in.nextString());
			}
		}
		in.endObject();

		// the frame is not cached
		// but we store the frame name
		// set it in the QS if a
		// context insight is defined
		if (this.insight != null) {
			if (qs.getFrameName() != null) {
				qs.setFrame((ITableDataFrame) this.insight.getVar(qs.getFrameName()));
			}
		}

		return qs;
	}

	@Override
	public void write(JsonWriter out, HardSelectQueryStruct value) throws IOException {
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
		if (value.getEngineId() != null) {
			out.name("engineName").value(value.getEngineId());
		}
		if (value.getFrameName() != null) {
			out.name("frameName").value(value.getFrameName());
			out.name("frameType").value(value.getFrameType());
		}
		out.name("query").value(value.getQuery());

		out.endObject();
	}
}
