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
package prerna.reactor.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.IRemoteClientServer;
import prerna.cluster.util.RemoteModelInfo;
import prerna.cluster.util.ZKClientFactory;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MyRemoteModelsStatus extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(MyRemoteModelsStatus.class);

  @Override
  public NounMetadata execute() {
    final IRemoteClientServer zkClient = ZKClientFactory.getZKClient(false);

    List<RemoteModelInfo> activeModels = zkClient.getActiveModels();
    List<RemoteModelInfo> warmingModels = zkClient.getWarmingModels();

    List<RemoteModelInfo> myActiveModels = new ArrayList<>();
    List<RemoteModelInfo> myWarmingModels = new ArrayList<>();

    for (RemoteModelInfo model : activeModels) {
      if (SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), model.getId())) {
        myActiveModels.add(model);
      }
    }

    for (RemoteModelInfo model : warmingModels) {
      if (SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), model.getId())) {
        myWarmingModels.add(model);
      }
    }

    Map<String, List<RemoteModelInfo>> modelsMap = new HashMap<>();
    modelsMap.put("activeModels", myActiveModels);
    modelsMap.put("warmingModels", myWarmingModels);

    return new NounMetadata(modelsMap, PixelDataType.MAP, PixelOperationType.OPERATION);
  }
}
