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
package prerna.reactor.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class BuildModelToolsArrayReactor extends AbstractReactor {

  public BuildModelToolsArrayReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    List<String> engineIds = getEngineIds();
    List<Map<String, Object>> toolsList = new ArrayList<>();
    for (String eId : engineIds) {
      if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), eId)) {
        throw new IllegalArgumentException(
            "Engine " + eId + " does not exist or user does not have access to the engine");
      }

      IEngine engine = Utility.getEngine(eId);
      toolsList.add(engine.buildOpenAIFunctionEngineToolMap());
    }

    return new NounMetadata(toolsList, PixelDataType.CUSTOM_DATA_STRUCTURE);
  }

  /**
   * @return list of engine ids
   */
  public List<String> getEngineIds() {
    List<String> engineIds = new ArrayList<>();

    // see if added as key
    GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.ENGINE.getKey());
    if (grs != null && !grs.isEmpty()) {
      int size = grs.size();
      for (int i = 0; i < size; i++) {
        engineIds.add(grs.get(i).toString());
      }
      return engineIds;
    }

    if (this.curRow != null && !this.curRow.isEmpty()) {
      int size = this.curRow.size();
      for (int i = 0; i < size; i++) {
        engineIds.add(this.curRow.get(i).toString());
      }
      return engineIds;
    }

    return engineIds;
  }

  @Override
  public String getReactorDescription() {
    return "Return an array with the tools maps for each engine id being requested";
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
      return "List of engine ids to generate their tools map and flush into an array";
    }
    return super.getDescriptionForKey(key);
  }
}
