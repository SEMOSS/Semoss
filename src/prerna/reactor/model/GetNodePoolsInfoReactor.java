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

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.model.KubernetesModelScaler;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetNodePoolsInfoReactor extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(GetNodePoolsInfoReactor.class);

  @Override
  public NounMetadata execute() {
    if (!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
      throw new IllegalArgumentException("User does not have permission to query this endpoint.");
    }

    final KubernetesModelScaler kmsServer;
    kmsServer = KubernetesModelScaler.getInstance();

    try {
      Map<String, Object> nodePoolsInfo = kmsServer.getNodePoolsInfo();
      return new NounMetadata(nodePoolsInfo, PixelDataType.MAP, PixelOperationType.OPERATION);
    } catch (Exception e) {
      classLogger.error(
          "Error connecting to the Kubernetes Model Scaler endpoint for Nodepool information..");
      throw new RuntimeException(
          "Failed to connect to Kubernetes Model Scaler endpoint: " + e.getMessage());
    }
  }
}
