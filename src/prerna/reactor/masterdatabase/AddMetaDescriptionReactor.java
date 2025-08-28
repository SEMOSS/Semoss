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
package prerna.reactor.masterdatabase;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.masterdatabase.AddToMasterDB;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class AddMetaDescriptionReactor extends AbstractMetaDBReactor {

  /**
   * This reactor adds a description to the concept metadata table The inputs to the reactor are: 1)
   * the engine 2) the the concept 3) the description to be added
   */
  public AddMetaDescriptionReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(), DESCRIPTION
        };
  }

  @Override
  public NounMetadata execute() {
    String engineId = getEngineId();
    engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
    if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
      throw new IllegalArgumentException(
          "App does not exist or user does not have access to edit database");
    }

    String concept = getConcept();
    String description = getDescription();
    AddToMasterDB master = new AddToMasterDB();
    boolean success = master.addMetadata(engineId, concept, Constants.DESCRIPTION, description);
    return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.DATABASE_INFO);
  }
}
