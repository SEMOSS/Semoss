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
package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.NerModelEngineResponse;
import prerna.om.Insight;

/** Named Entity Recognition models */
public class NEREngine extends AbstractRemoteModelEngine {

  private static final Logger classLogger = LogManager.getLogger(NEREngine.class);

  public NerModelEngineResponse predict(
      String text,
      List<String> entities,
      List<String> maskEntities,
      Insight insight,
      Map<String, Object> parameters) {
    JSONObject payload = new JSONObject();
    payload.put("text", text);
    payload.put("labels", entities);

    if (maskEntities != null && !maskEntities.isEmpty()) {
      payload.put("mask_entities", maskEntities);
    }

    payload.put("model", this.model);

    classLogger.debug("NER predict payload: {}", payload.toString(2));

    try {
      JSONObject response = this.makeModelRequest(payload);

      if (response == null) {
        Map<String, Object> errorMap = new HashMap<>();
        errorMap.put("status", "error");
        errorMap.put("message", "Null response from model");
        return new NerModelEngineResponse(errorMap, 0, 0);
      }

      if (response.has("status") && "error".equals(response.getString("status"))) {
        Map<String, Object> errorMap = new HashMap<>();
        errorMap.put("status", "error");
        errorMap.put("message", response.optString("message", "Unknown error"));
        errorMap.put("code", response.optInt("code", 0));
        return new NerModelEngineResponse(errorMap, 0, 0);
      }

      NerModelEngineResponse formattedResponse = NerModelEngineResponse.fromJson(response);
      return formattedResponse;
    } catch (Exception e) {
      classLogger.error("Error making model request", e);
      Map<String, Object> errorMap = new HashMap<>();
      errorMap.put("status", "error");
      errorMap.put("message", e.getMessage());

      return new NerModelEngineResponse(errorMap, 0, 0);
    }
  }

  @Override
  public ModelTypeEnum getModelType() {
    return ModelTypeEnum.NER;
  }
}
