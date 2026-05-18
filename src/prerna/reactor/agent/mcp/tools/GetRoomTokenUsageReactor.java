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
package prerna.reactor.agent.mcp.tools;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Returns the total token count for the current room message history. */
public class GetRoomTokenUsageReactor extends AbstractReactor {

  public GetRoomTokenUsageReactor() {
    this.keysToGet = new String[] { "roomId" };
    this.keyRequired = new int[] { 0 };
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String roomId = this.keyValue.get("roomId");
    if (roomId == null || roomId.trim().isEmpty()) {
      roomId = insight.getRoomId();
    }
    if (roomId == null || roomId.trim().isEmpty()) {
      throw new IllegalArgumentException("roomId is required to calculate token usage");
    }

    Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
    List<AbstractMessage> messages = room.getMessages();
    int totalTokens = 0;
    totalTokens = messages.parallelStream().mapToInt(AbstractMessage::getTokensInMessage).sum();

    Map<String, Object> result = new HashMap<>();
    result.put("roomId", roomId);
    result.put("messageCount", messages.size());
    result.put("totalTokens", totalTokens);
    return new NounMetadata(result, PixelDataType.MAP);
  }

  @Override
  public String getReactorDescription() {
    return "Returns the total token count for the current room message history.";
  }
}