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
package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetRoomMessagesReactor extends AbstractReactor {
  @SuppressWarnings("unused")
  private static final Logger logger = LogManager.getLogger(GetRoomMessagesReactor.class);

  public GetRoomMessagesReactor() {
    this.keysToGet = new String[] {"roomId", "limit", "offset", "sort"};
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    User user = this.insight.getUser();
    if (user == null) {
      throw new IllegalArgumentException("You are not properly logged in");
    }
    Integer limit = -1, offset = -1;
    String roomId = this.keyValue.get(this.keysToGet[0]);
    String dateSort = "ASC";

    if ((this.keyValue.get(this.keysToGet[1]) != null
            && !this.keyValue.get(this.keysToGet[1]).isEmpty())
        && (this.keyValue.get(this.keysToGet[2]) != null
            && !this.keyValue.get(this.keysToGet[2]).isEmpty())) {
      limit = Integer.parseInt(this.keyValue.get(this.keysToGet[1]));
      offset = Integer.parseInt(this.keyValue.get(this.keysToGet[2]));
    }
    if (this.keyValue.get(this.keysToGet[3]) != null
        && !this.keyValue.get(this.keysToGet[3]).isEmpty()
        && this.keyValue.get(this.keysToGet[3]).equals("DESC")) dateSort = "DESC";

    List<Map<String, Object>> output =
        ModelInferenceLogsUtils.doRetrieveConversation(
            user.getPrimaryLoginToken().getId(), roomId, dateSort, limit, offset);
    return new NounMetadata(output, PixelDataType.VECTOR);
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals("roomId")) {
      return "The room or conversation ID for a given chat app";
    }
    return super.getDescriptionForKey(key);
  }
}
