/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.web;

import java.util.Hashtable;
import java.util.List;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.json.JsonWrapper;
import prerna.om.HeadersDataRow;

public class WebWrapper extends JsonWrapper {

	private List<String[]> rows = null;

	@Override
	public void execute() throws Exception {
		// sorry for the bad way to transport data
		Hashtable output = (Hashtable) engine.execQuery(query);
		rows = (List) output.get("ROWS");
		headers = (String[]) output.get("HEADERS");
		this.numColumns = this.headers.length;
		numRows = rows.size();
		String[] strTypes = (String[]) output.get("TYPES");
		this.types = new SemossDataType[this.numColumns];
		for (int i = 0; i < this.numColumns; i++) {
			this.types[i] = SemossDataType.convertStringToDataType(strTypes[i]);
		}
	}

	@Override
	public IHeadersDataRow next() {
		IHeadersDataRow retRow = new HeadersDataRow(headers, rows.get(curRow));
		curRow++;
		return retRow;
	}
}
