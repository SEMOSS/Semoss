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
package prerna.usertracking.reactors;

import java.util.ArrayList;
import java.util.List;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.EngineUsageUtils;
import prerna.util.Utility;

public class TrendingDatabasesReactor extends AbstractReactor {

  public TrendingDatabasesReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.NUM_DISPLAY.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    if (Utility.isUserTrackingDisabled()) {
      return new NounMetadata(
          false, PixelDataType.BOOLEAN, PixelOperationType.USER_TRACKING_DISABLED);
    }

    List<String> eTypes = new ArrayList<>();
    eTypes.add(IEngine.CATALOG_TYPE.DATABASE.toString());

    String numDisplay = this.keyValue.get(this.keysToGet[0]);
    if (numDisplay == null) {
      numDisplay = "5";
    }
    Integer nd = Integer.valueOf(numDisplay);

    List<String> accessibleDbs =
        SecurityEngineUtils.getUserEngineIdList(this.insight.getUser(), eTypes, true, true, true);

    List<String> dbs = EngineUsageUtils.getTrendingDatabases(nd, accessibleDbs);

    // just fill out the rest of the trending databases for consistency.
    // Netflix has terrible recommendations too :)
    if (dbs.size() < nd) {
      accessibleDbs.removeAll(dbs);
      int size = accessibleDbs.size();
      int toAdd = Math.min(size, nd - dbs.size());
      for (int i = 0; i < toAdd; i++) {
        dbs.add(accessibleDbs.get(i));
      }
    }

    return new NounMetadata(
        dbs, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_LIST);
  }
}
