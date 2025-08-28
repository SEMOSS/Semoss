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
package prerna.reactor.export;

import java.util.HashMap;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class FrameCacheReactor extends AbstractReactor {

  public FrameCacheReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.FRAME_CACHE.getKey()};
  }

  public NounMetadata execute() {
    // default this to use Python
    // if Python not present
    // try in R
    // default is R
    // reset it ?

    if (insight.getPragmap() == null) {
      insight.setPragmap(new HashMap());
    }
    this.insight.getPragmap().put("xCache", this.curRow.vector.get(0).getValue());

    boolean value = Boolean.parseBoolean(insight.getPragmap().get("xCache") + "");
    NounMetadata noun =
        new NounMetadata(value, PixelDataType.BOOLEAN, PixelOperationType.FRAME_CACHE);
    noun.addAdditionalReturn(getSuccess("Cache is now set to " + value));
    return noun;
  }
}
