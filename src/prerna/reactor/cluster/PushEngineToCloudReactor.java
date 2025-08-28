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
package prerna.reactor.cluster;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PushEngineToCloudReactor extends AbstractReactor {

  public PushEngineToCloudReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String engineId = this.keyValue.get(this.keysToGet[0]);

    if (engineId == null || engineId.isEmpty()) {
      throw new IllegalArgumentException("Must input an engine id");
    }

    // make sure valid id for user
    if (!SecurityEngineUtils.userIsOwner(this.insight.getUser(), engineId)) {
      // you dont have access
      throw new IllegalArgumentException(
          "Engine does not exist or user is not an owner to force pushing to cloud storage");
    }

    ClusterUtil.pushEngine(engineId);
    return new NounMetadata(true, PixelDataType.BOOLEAN);
  }
}
