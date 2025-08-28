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

import java.util.Properties;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetModelMaxTokenLength extends AbstractReactor {

  public GetModelMaxTokenLength() {
    this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String engineId = this.keyValue.get(this.keysToGet[0]);
    if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
      throw new IllegalArgumentException(
          "Vector db " + engineId + " does not exist or user does not have access to this engine");
    }

    IModelEngine modelEngine = Utility.getModel(engineId);
    Properties modelProperties = modelEngine.getSmssProp();

    String maxTokens = "512";
    if (!modelProperties.isEmpty() && modelProperties.containsKey(Constants.MAX_TOKENS)) {
      maxTokens = modelProperties.getProperty(Constants.MAX_TOKENS);
    }

    return new NounMetadata(maxTokens, PixelDataType.CONST_STRING);
  }
}
