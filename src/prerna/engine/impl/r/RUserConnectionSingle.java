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
import prerna.util.Constants;

public class RUserConnectionSingle extends AbstractRUserConnection {

  private static final Logger classLogger = LogManager.getLogger(RUserConnectionSingle.class);

  // Host and port
  private final String host;
  private static final String DEFAULT_HOST = "localhost";
  private int port = 6311;

  // TODO >>>timb: R - this constructor is never used, so the host is not really configurable right
  // now (later)
  public RUserConnectionSingle(String rDataFile, String host, int myPort) {
    super(rDataFile);
    this.host = host;
    this.port = myPort;
  }

  public RUserConnectionSingle(String rDataFile) {
    super(rDataFile);
    this.host = DEFAULT_HOST;
  }

  public RUserConnectionSingle() {
    super();
    this.host = DEFAULT_HOST;
  }

  @Override
  public void initializeConnection() throws Exception {
    rcon = RserveUtil.connect(host, port);
  }

  @Override
  protected void recoverConnection() throws Exception {
    try {
      stopR();
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
    }
    initializeConnection();
    loadDefaultPackages();
    // Make sure R is healthy
    if (!isHealthy()) {
      throw new IllegalArgumentException("Basic R heath check failed after restarting R.");
    }
    this.stoppedR = false;
  }

  @Override
  public void stopR() throws Exception {
    if (rcon != null) {
      rcon.close();
    }
    this.stoppedR = true;
  }

  @Override
  public void cancelExecution() throws Exception {
    // TODO Auto-generated method stub

  }
}
