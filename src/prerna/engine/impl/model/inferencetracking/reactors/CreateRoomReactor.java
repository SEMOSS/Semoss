/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CreateRoomReactor extends AbstractReactor {

  public CreateRoomReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.NAME.getKey(),
          ReactorKeysEnum.CONTEXT.getKey(),
          ReactorKeysEnum.VECTORDB.getKey(),
          ReactorKeysEnum.FUNCTION.getKey(),
          ReactorKeysEnum.WORKSPACE_ID.getKey()
        };
    this.keyRequired = new int[] {0, 0, 0, 0, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String roomName = this.keyValue.get(ReactorKeysEnum.NAME.getKey());
    String context = this.keyValue.get(ReactorKeysEnum.CONTEXT.getKey());
    String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());

    Map<String, Object> options = null;

    if (workspaceId == null) {
      List<String> vectorDbs = getVectorDbs();
      List<String> tools = getTools();
      if (!tools.isEmpty() || !vectorDbs.isEmpty()) {
        options = new HashMap<>();
        if (!tools.isEmpty()) {
          options.put("tools", tools);
        }
        if (!vectorDbs.isEmpty()) {
          options.put("vectorDbs", vectorDbs);
        }
      }
    }

    Room room =
        RoomUtils.createRoomIfNotExists(
            UUID.randomUUID().toString(), insight, null, roomName, workspaceId, options, context);
    Map<String, Object> output = new HashMap<String, Object>();
    output.put("roomId", room.getId());
    return new NounMetadata(output, PixelDataType.MAP);
  }

  private List<String> getVectorDbs() {
    List<String> inputStrings = new ArrayList<>();
    GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.VECTORDB.getKey());
    if (grs != null && !grs.isEmpty()) {
      int size = grs.size();
      for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
      return inputStrings;
    }
    int size = this.curRow.size();
    for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
    return inputStrings;
  }

  private List<String> getTools() {
    List<String> inputStrings = new ArrayList<>();
    GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.FUNCTION.getKey());
    if (grs != null && !grs.isEmpty()) {
      int size = grs.size();
      for (int i = 0; i < size; i++) inputStrings.add(grs.get(i).toString());
      return inputStrings;
    }
    int size = this.curRow.size();
    for (int i = 0; i < size; i++) inputStrings.add(this.curRow.get(i).toString());
    return inputStrings;
  }
}
