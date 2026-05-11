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
package prerna.reactor.qs;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.ReactorKeysEnum;

public class QueryReactor extends AbstractQueryStructReactor {

	public QueryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_KEY.getKey() };
	}

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		organizeKeys();
		// grab the query
		String query = this.keyValue.get(this.keysToGet[0]);

		// create a new query struct
		HardSelectQueryStruct hardQs = null;
		if (this.qs instanceof HardSelectQueryStruct) {
			// we already have some form of a hard qs
			// so just use the existing one
			// and set the query
			hardQs = (HardSelectQueryStruct) this.qs;

		} else if (this.qs instanceof SelectQueryStruct) {
			SelectQueryStruct sQs = ((SelectQueryStruct) qs);

			if (sQs.getQsType() == QUERY_STRUCT_TYPE.ENGINE) {
				hardQs = new HardSelectQueryStruct();
				hardQs.setEngine(qs.getEngine());
				hardQs.setEngineId(qs.getEngineId());
				hardQs.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
			} else {
				hardQs = new HardSelectQueryStruct();
				hardQs.setFrame(qs.getFrame());
				hardQs.setQsType(QUERY_STRUCT_TYPE.RAW_FRAME_QUERY);
			}
		}

		if (hardQs == null) {
			throw new NullPointerException("HardSelectQueryStruct hardQs should not be null here.");
		}

		hardQs.setQuery(query);
		// override it with new query struct
		hardQs.setBigDataEngine(this.qs.getBigDataEngine());
		hardQs.setPragmap(this.qs.getPragmap());
		this.qs = hardQs;

		return this.qs;
	}

}
