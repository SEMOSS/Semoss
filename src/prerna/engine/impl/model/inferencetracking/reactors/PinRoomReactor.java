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

import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PinRoomReactor extends AbstractReactor {

  public PinRoomReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.PINNED.getKey()};
    this.keyRequired = new int[] {1, 1};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
    boolean pinned = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.PINNED.getKey()));

    boolean result =
        ModelInferenceLogsUtils.doSetRoomToPinned(
            insight.getUser().getPrimaryLoginToken().getId(), roomId, pinned);

    return new NounMetadata(result, PixelDataType.BOOLEAN);
  }
}
