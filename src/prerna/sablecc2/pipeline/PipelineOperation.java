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
package prerna.sablecc2.pipeline;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class PipelineOperation {

	List<Map> rowInputs = new Vector<Map>();
	Map<String, List<Map>> nounInputs = new Hashtable<String, List<Map>>();

	// the name of the reactor
	String opName = null;
	String opString = null;
	String widgetId = null;

	/**
	 * Constructor
	 *
	 * @param opName
	 *            The name of the reactor for the operation
	 * @param opString
	 *            Primarily need this for debugging
	 */
	public PipelineOperation(String opName, String opString) {
		this.opName = opName;
		this.opString = opString;
	}

	public String getOpName() {
		return this.opName;
	}

	public String getOpString() {
		return this.opString;
	}

	public void setWidgetId(String widgetId) {
		this.widgetId = widgetId;
	}

	public String getWidgetId() {
		return this.widgetId;
	}

	public void addRowInput(Map o) {
		this.rowInputs.add(o);
	}

	public void addNounInputs(String key, Map o) {
		if (this.nounInputs.containsKey(key)) {
			this.nounInputs.get(key).add(o);
		} else {
			List<Map> oList = new Vector<Map>();
			oList.add(o);
			this.nounInputs.put(key, oList);
		}
	}

	public List<Map> getRowInputs() {
		return this.rowInputs;
	}

	public Map<String, List<Map>> getNounInputs() {
		return this.nounInputs;
	}

	public void setNounInputs(Map<String, List<Map>> nounInputs) {
		this.nounInputs = nounInputs;
	}
}
