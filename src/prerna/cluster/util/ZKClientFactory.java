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
package prerna.cluster.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Factory class to get the appropriate ZooKeeper client implementation based on the environment
 * configuration.
 */
public class ZKClientFactory {

  private static final Logger classLogger = LogManager.getLogger(ZKClientFactory.class);

  /**
   * Gets the appropriate ZooKeeper client implementation based on environment variables.
   *
   * @return An implementation of IRemoteClientServer
   */
  public static IRemoteClientServer getZKClient(Boolean devPortForwarding) {
    String zkIngress = System.getenv("ZK_INGRESS");

    if (zkIngress != null && !zkIngress.isEmpty() && !devPortForwarding) {
      classLogger.info("ZK_INGRESS found, using REST proxy connection for ZooKeeper");

      // When using ZK REST proxy, KMS_INGRESS is required
      String kmsIngress = System.getenv("KMS_INGRESS");
      if (kmsIngress == null || kmsIngress.isEmpty()) {
        classLogger.warn(
            "KMS_INGRESS environment variable is not set but required for ZK REST Proxy mode");
      }

      return RemoteClientServerZKRESTProxy.getInstance();
    } else {
      classLogger.info("Using direct ZooKeeper connection (cluster mode)");
      return RemoteClientServerZK.getInstance();
    }
  }
}
