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
package prerna.reactor.venv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.om.ClientProcessWrapper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;

public class ActivateVirtualEnvReactor extends AbstractReactor {

  private static Logger classLogger = LogManager.getLogger(ActivateVirtualEnvReactor.class);

  public ActivateVirtualEnvReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
    this.keyRequired = new int[] {1};
  }

  public NounMetadata execute() {
    this.organizeKeys();

    String venvName = this.keyValue.get(this.keysToGet[0]);

    User user = this.insight.getUser();
    if (user == null) {
      return NounMetadata.getErrorNounMessage("Cannot restart server. User not valid");
    }

    // sadly, the logic right now requires we have a made cpw
    // otherwise the reconnect method does nto
    ClientProcessWrapper cpw = user.getPythonClientProcessWrapper();
    if (cpw == null || cpw.getSocketClient() == null) {
      user.getPythonSocketClient(true, venvName);
      return new NounMetadata(
          "TCP Server was not initialized but is now started and connected",
          PixelDataType.CONST_STRING);
    }
    cpw.shutdown(false);
    try {
      cpw.reconnect(venvName);
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
      return new NounMetadata("Unable to restart TCP Server", PixelDataType.CONST_STRING);
    }
    SocketClient client = user.getPythonSocketClient(false);
    if (client == null || !client.isConnected()) {
      return new NounMetadata("Unable to restart TCP Server", PixelDataType.CONST_STRING);
    }

    return new NounMetadata("TCP Server available and connected", PixelDataType.CONST_STRING);
  }
}
