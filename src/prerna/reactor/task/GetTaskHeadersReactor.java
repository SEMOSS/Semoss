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
package prerna.reactor.task;

import java.util.List;
import java.util.Map;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

public class GetTaskHeadersReactor extends AbstractReactor {

	public GetTaskHeadersReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey()};
	}

	@Override
	public NounMetadata execute() {
		List<Map<String, Object>> headerInfo = null;
		Object obj = this.curRow.get(0);
		if (obj instanceof ITask) {
			headerInfo = ((ITask) obj).getHeaderInfo();
		} else if (obj instanceof Map) {
			headerInfo = (List<Map<String, Object>>) ((Map) obj).get("headerInfo");
		}
		String[] headers = new String[headerInfo.size()];
		for (int i = 0; i < headerInfo.size(); i++) {
			headers[i] = (String) headerInfo.get(i).get("alias");
		}
		return new NounMetadata(headers, PixelDataType.CONST_STRING);
	}
}
