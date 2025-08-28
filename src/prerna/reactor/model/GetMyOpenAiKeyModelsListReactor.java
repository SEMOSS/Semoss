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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;
import org.json.JSONArray;
import org.json.JSONObject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.utils.GetRequestReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetMyOpenAiKeyModelsListReactor extends AbstractReactor {

  private static final String OPENAI_MODEL_LIST_ENDPOINT = "https://api.openai.com/v1/models";

  public GetMyOpenAiKeyModelsListReactor() {
    this.keysToGet = new String[] {"openaiKey", "organizationId"};
    this.keyRequired = new int[] {1, 0};
  }

  @Override
  public NounMetadata execute() {
    this.organizeKeys();
    String openaiKey = this.keyValue.get(this.keysToGet[0]);
    String organizationId = this.keyValue.get(this.keysToGet[1]);

    NounStore ns = new NounStore(ReactorKeysEnum.ALL.getKey());
    ns.makeNoun(ReactorKeysEnum.URL.getKey()).addLiteral(OPENAI_MODEL_LIST_ENDPOINT);

    Map<Object, Object> headersMap = new HashMap<>();
    headersMap.put("Authorization", "Bearer " + openaiKey);
    if (organizationId != null && !organizationId.isEmpty()) {
      headersMap.put("OpenAI-Organization", organizationId);
    }
    ns.makeNoun("headersMap").addMap(headersMap);
    GetRequestReactor getRequest = new GetRequestReactor();
    getRequest.setInsight(this.insight);
    getRequest.setNounStore(ns);
    getRequest.In();
    NounMetadata requestResponse = getRequest.execute();
    if (requestResponse.getNounType().equals(PixelDataType.ERROR)) { // server side errors
      throw new IllegalArgumentException("ERROR: " + requestResponse.getExplanation());
    }

    String responseString = (String) requestResponse.getValue();
    JSONObject response = new JSONObject(responseString);
    JSONArray modelDetails = response.getJSONArray("data");
    HashSet<String> modelList = new HashSet<>();
    for (int i = 0; i < modelDetails.length(); i++) {
      JSONObject modleInfo = modelDetails.getJSONObject(i);
      modelList.add(modleInfo.getString("id"));
    }

    return new NounMetadata(new TreeSet<>(modelList).toArray(), PixelDataType.VECTOR);
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals("openaiKey")) {
      return "Open AI Key obtained from https://platform.openai.com/account/api-keys";
    } else if (key.equals("organizationId")) {
      return "(Optional) For users who belong to multiple organizations, you can pass a header to specify which organization is used for an API request";
    }

    return super.getDescriptionForKey(key);
  }
}
