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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.AbstractRemoteModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RemoteModelStartReactor extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(RemoteModelStartReactor.class);

  public RemoteModelStartReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String engineId = this.keyValue.get(this.keysToGet[0]);

    if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
      throw new IllegalArgumentException(
          "Model " + engineId + " does not exist or user does not have access to this model");
    }

    try {
      IModelEngine targetModel = Utility.getModel(engineId);
      AbstractRemoteModelEngine targetEngine = (AbstractRemoteModelEngine) targetModel;

      Boolean startUpResult = targetEngine.initiateAndWaitForDeployment(200000);

      String result;

      if (startUpResult) {
        result = "Model started successfully";
      } else {
        result = "Model failed to start";
      }

      return new NounMetadata(result, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
    } catch (Exception e) {
      classLogger.error("Error starting model: " + engineId, e);
      throw new RuntimeException("Failed to start model: " + e.getMessage());
    }
  }
}
