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
package prerna.reactor.security;

import java.util.List;
import java.util.Map;
import java.util.Vector;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetSpecificInsightMetaReactor extends AbstractReactor {

  private static List<String> META_KEYS_LIST = new Vector<String>();

  static {
    META_KEYS_LIST.add("description");
    META_KEYS_LIST.add("tag");
  }

  public GetSpecificInsightMetaReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String projectId = this.keyValue.get(this.keysToGet[0]);
    String rdbmsId = this.keyValue.get(this.keysToGet[1]);

    projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
    if (!SecurityInsightUtils.userCanViewInsight(this.insight.getUser(), projectId, rdbmsId)) {
      NounMetadata noun =
          new NounMetadata(
              "User does not have access to this insight",
              PixelDataType.CONST_STRING,
              PixelOperationType.ERROR);
      SemossPixelException err = new SemossPixelException(noun);
      err.setContinueThreadOfExecution(false);
      throw err;
    }

    Map<String, Object> retMap =
        SecurityInsightUtils.getSpecificInsightMetadata(projectId, rdbmsId, META_KEYS_LIST);
    retMap.putIfAbsent("description", "");
    retMap.putIfAbsent("tags", new Vector<String>());
    // put in cacheable and cacheMinutes
    retMap.putAll(SecurityInsightUtils.getSpecificInsightCacheDetails(projectId, rdbmsId));
    retMap.put("global", SecurityInsightUtils.insightIsGlobal(projectId, rdbmsId));

    NounMetadata retNoun = new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
    return retNoun;
  }
}
