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
package prerna.reactor.export;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import prerna.engine.api.IHeadersDataRow;

public class TableFormatter extends AbstractFormatter {

	public static final String FORMAT_TYPE = "TABLE";

	private List<Object[]> data;
	private String[] headers;
	private String[] rawHeaders;

	public TableFormatter() {
		this.data = new ArrayList<>(100);
		this.headers = new String[0];
		this.rawHeaders = new String[0];
	}

	@Override
	public void addData(IHeadersDataRow nextData) {
		this.headers = nextData.getHeaders();
		this.rawHeaders = nextData.getRawHeaders();
		this.data.add(nextData.getValues());
	}

	public Object getFormattedData() {
		Map<String, Object> returnData = new Hashtable<String, Object>();
		returnData.put("values", this.data);
		returnData.put("headers", this.headers);
		returnData.put("rawHeaders", this.rawHeaders);
		return returnData;
	}

	@Override
	public void clear() {
		this.data = new ArrayList<>(100);
		this.headers = new String[0];
	}

	@Override
	public String getFormatType() {
		return TableFormatter.FORMAT_TYPE;
	}

	public String[] getHeaders() {
		return this.headers;
	}

	public List<Object[]> getData() {
		return this.data;
	}
}
