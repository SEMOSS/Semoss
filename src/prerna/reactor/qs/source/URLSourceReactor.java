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
package prerna.reactor.qs.source;

import java.util.Map;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class URLSourceReactor extends AbstractQueryStructReactor {

	public URLSourceReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey(), ReactorKeysEnum.DELIMITER.getKey()};
	}

	@Override
	protected SelectQueryStruct createQueryStruct() {
		// get inputs
		organizeKeys();
		String urlInput = this.keyValue.get(this.keysToGet[0]);
		String delimiterStr = this.keyValue.get(this.keysToGet[1]);

		// default delimiter or use the one defined
		char delim = ',';
		if (delimiterStr != null && delimiterStr.length() > 0) {
			delim = delimiterStr.charAt(0);
		}

		// flush out url to file
		String filePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		filePath += "\\" + Utility.getRandomString(10) + ".csv";
		filePath = filePath.replace("\\", "/");
		Utility.copyURLtoFile(urlInput, filePath);

		// get datatypes
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(',');
		helper.parse(filePath);
		Map[] predictionMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(helper.getHeaders(),
				helper.predictTypes());
		Map<String, String> dataTypes = predictionMaps[0];
		Map<String, String> additionalDataTypes = predictionMaps[1];
		CsvQueryStruct qs = new CsvQueryStruct();
		for (String key : dataTypes.keySet()) {
			qs.addSelector("DND", key);
		}
		helper.clear();
		qs.merge(this.qs);
		qs.setFilePath(filePath);
		qs.setDelimiter(delim);
		qs.setColumnTypes(dataTypes);
		qs.setAdditionalTypes(additionalDataTypes);
		return qs;
	}
}
