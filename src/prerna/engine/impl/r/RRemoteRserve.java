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
package prerna.engine.impl.r;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;
import prerna.util.Constants;

public class RRemoteRserve {

  private static final Logger classLogger = LogManager.getLogger(RRemoteRserve.class);

  private RConnection rcon = null;

  public RConnection getConnection() {

    if (Boolean.parseBoolean(System.getenv("REMOTE_RSERVE"))) {
      String server = System.getenv("REMOTE_RSERVE_IP");
      if (server.contains(":")) {
        String[] hostInfo = server.split(":", 2);
        try {
          String host = hostInfo[0];
          int port = Integer.parseInt(hostInfo[1]);
          rcon = new RConnection(host, port);
        } catch (RserveException e) {
          // TODO Auto-generated catch block
          classLogger.error(Constants.STACKTRACE, e);
        }
      } else {
        try {
          rcon = new RConnection(server);
        } catch (RserveException e) {
          // TODO Auto-generated catch block
          classLogger.error(Constants.STACKTRACE, e);
        }
      }
    }
    return rcon;
  }
}
