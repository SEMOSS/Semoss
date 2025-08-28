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

import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.model.KubernetesModelScaler;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetRemoteModelDeployConfigsReactor extends AbstractReactor {

  private static final Logger classLogger =
      LogManager.getLogger(GetRemoteModelDeployConfigsReactor.class);

  public GetRemoteModelDeployConfigsReactor() {
    this.keysToGet = new String[] {"refresh"};
    this.keyRequired = new int[] {0};
  }

  @Override
  public NounMetadata execute() {
    if (!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
      throw new IllegalArgumentException("User does not have permission to query this endpoint.");
    }

    this.organizeKeys();
    Boolean refresh = this.getRefreshBool();

    final KubernetesModelScaler kmsServer;
    kmsServer = KubernetesModelScaler.getInstance();

    try {
      Map<String, Object> nodePoolsInfo = kmsServer.getModelDeploymentConfigs(refresh);
      return new NounMetadata(nodePoolsInfo, PixelDataType.MAP, PixelOperationType.OPERATION);
    } catch (Exception e) {
      classLogger.error(
          "Error connecting to the Kubernetes Model Scaler endpoint for model deployment configurations..");
      throw new RuntimeException(
          "Failed to connect to Kubernetes Model Scaler endpoint: " + e.getMessage());
    }
  }

  private boolean getRefreshBool() {
    GenRowStruct boolGrs = this.store.getNoun("refresh");
    if (boolGrs != null) {
      if (boolGrs.size() > 0) {
        List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
        return (boolean) val.get(0);
      }
    }
    return false;
  }
}
