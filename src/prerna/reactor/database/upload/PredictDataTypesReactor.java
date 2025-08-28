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
package prerna.reactor.database.upload;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.Logger;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

public class PredictDataTypesReactor extends AbstractReactor {

  private static final String CLASS_NAME = PredictDataTypesReactor.class.getName();

  public PredictDataTypesReactor() {
    this.keysToGet =
        new String[] {
          UploadInputUtility.FILE_PATH,
          UploadInputUtility.SPACE,
          UploadInputUtility.DELIMITER,
          UploadInputUtility.ROW_COUNT
        };
  }

  @Override
  public NounMetadata execute() {
    Logger logger = getLogger(CLASS_NAME);
    logger.info("Extracting file headers and determinig data types");
    organizeKeys();
    String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
    if (!new File(filePath).exists()) {
      throw new IllegalArgumentException("Unable to locate file");
    }

    String delimiter = UploadInputUtility.getDelimiter(this.store);
    char delim = delimiter.charAt(0);
    boolean rowCount = UploadInputUtility.getRowCount(this.store);
    CSVFileHelper helper = new CSVFileHelper();
    Map<String, Object> retMap = new HashMap<>();
    try {
      helper.setLogger(logger);
      helper.setDelimiter(delim);
      helper.parse(filePath);
      Map[] predictionMaps =
          FileHelperUtil.generateDataTypeMapsFromPrediction(
              helper.getHeaders(), helper.predictTypes());
      retMap.put("headers", helper.getFileOriginalHeaders());
      retMap.put("cleanHeaders", helper.getHeaders());
      retMap.put("dataTypes", predictionMaps[0]);
      retMap.put("additionalDataTypes", predictionMaps[1]);
      if (rowCount) {
        // get the row count
        int count = 1;
        while ((helper.getNextRow()) != null) {
          count++;
        }
        retMap.put("endRow", count);
      }
    } finally {
      helper.clear();
    }
    return new NounMetadata(retMap, PixelDataType.MAP);
  }
}
