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
package prerna.reactor.vector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ListDocumentsInVectorDatabaseReactor extends AbstractReactor {

  public ListDocumentsInVectorDatabaseReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata execute() {
    this.organizeKeys();
    String engineId = this.keyValue.get(this.keysToGet[0]);
    if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
      throw new IllegalArgumentException(
          "Vector database "
              + engineId
              + " does not exist or user does not have access to this vector database");
    }

    Map<String, Object> paramMap = getMap();
    if (paramMap == null) {
      paramMap = new HashMap<String, Object>();
    }

    IVectorDatabaseEngine engine = Utility.getVectorDatabase(engineId);
    if (engine == null) {
      throw new SemossPixelException("Unable to find engine");
    }

    return new NounMetadata(
        engine.listDocuments(paramMap),
        PixelDataType.CUSTOM_DATA_STRUCTURE,
        PixelOperationType.OPERATION);
  }

  private Map<String, Object> getMap() {
    GenRowStruct mapGrs = this.store.getNoun(keysToGet[1]);
    if (mapGrs != null && !mapGrs.isEmpty()) {
      List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
      if (mapInputs != null && !mapInputs.isEmpty()) {
        return (Map<String, Object>) mapInputs.get(0).getValue();
      }
    }
    List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
    if (mapInputs != null && !mapInputs.isEmpty()) {
      return (Map<String, Object>) mapInputs.get(0).getValue();
    }
    return null;
  }

  @Override
  public String getReactorDescription() {
    return "Get the unique list of 'Source' values that have been uploaded into this vector database. "
        + "Typically the 'Souce' represents the files that have been uploaded, but in the case of custom chunking "
        + "the value will be returned even if the file itself was never uploaded";
  }
}
