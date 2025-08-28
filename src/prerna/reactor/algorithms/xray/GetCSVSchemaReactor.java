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
package prerna.reactor.algorithms.xray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetCSVSchemaReactor extends AbstractReactor {

  public GetCSVSchemaReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.DELIMITER.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    // get inputs
    String filePath = this.keyValue.get(this.keysToGet[0]);
    if (filePath == null) {
      throw new IllegalArgumentException(
          "Need to define the " + ReactorKeysEnum.FILE_PATH.getKey());
    }
    String delimiter = this.keyValue.get(this.keysToGet[1]);
    if (delimiter == null) {
      throw new IllegalArgumentException(
          "Need to define the " + ReactorKeysEnum.DELIMITER.getKey());
    }

    CSVFileHelper cv = new CSVFileHelper();
    cv.setDelimiter(delimiter.charAt(0));
    cv.parse(filePath);
    String[] headers = cv.getAllCSVHeaders();
    Object[][] typePredictions = cv.predictTypes();

    Map<String, Object> ret = new HashMap<String, Object>();
    // generate db name
    String[] parts = filePath.split("\\\\");
    String dbName = parts[parts.length - 1].replace(".", "_");
    // C:\\..\\file.csv -> file_csv
    ret.put("databaseName", dbName);

    // construct empty relationship map (assuming flat table)
    Map<String, List<String>> relationshipMap = new HashMap<String, List<String>>();
    for (String concept : headers) {
      relationshipMap.put(concept, new ArrayList<String>());
    }
    ret.put("relationships", relationshipMap);

    // add column details
    // since it's a flat table we don't need to worry about concept/property
    // relationships
    Map<String, Map> tableDetails = new HashMap<String, Map>();
    for (int i = 0; i < headers.length; i++) {
      Map<String, String> colDetails = new HashMap<String, String>();
      colDetails.put("name", headers[i]);
      // index 1 is the data type as an enum
      colDetails.put("type", typePredictions[i][1].toString());
      tableDetails.put(headers[i], colDetails);
    }

    ret.put("tables", tableDetails);
    return new NounMetadata(
        ret, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
  }
}
