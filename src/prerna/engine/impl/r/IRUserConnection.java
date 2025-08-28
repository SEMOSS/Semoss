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
package prerna.engine.impl.r;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RSession;

public interface IRUserConnection {

  public static final String POOLED = "pooled";
  public static final String DEDICATED = "dedicated";
  public static final String SINGLE = "single";

  public static final String TYPE = RserveUtil.R_USER_CONNECTION_TYPE;

  public static IRUserConnection getRUserConnection(String rDataFile) {
    if (TYPE.equals(POOLED)) {
      return new RUserConnectionPooled(rDataFile);
    } else if (TYPE.equals(DEDICATED)) {
      return new RUserConnectionDedicated(rDataFile);
    } else if (TYPE.equals(SINGLE)) {
      return new RUserConnectionSingle(rDataFile);
    } else {
      throw new IllegalArgumentException("Unknown R user connection type: " + TYPE);
    }
  }

  public static IRUserConnection getRUserConnection() {
    if (TYPE.equals(POOLED)) {
      return new RUserConnectionPooled();
    } else if (TYPE.equals(DEDICATED)) {
      return new RUserConnectionDedicated();
    } else if (TYPE.equals(SINGLE)) {
      return new RUserConnectionSingle();
    } else {
      throw new IllegalArgumentException("Unknown R user connection type: " + TYPE);
    }
  }

  public void loadDefaultPackages() throws Exception;

  public void initializeConnection() throws Exception;

  /**
   * Really want to get rid of this; should not be manipulating the rcon directly outside of this
   * class
   *
   * @return
   */
  @Deprecated
  public RConnection getRConnection();

  public REXP eval(String rScript);

  public void voidEval(String rScript);

  public RSession detach();

  /**
   * Stops just the user-specific R process.
   *
   * @throws Exception
   */
  public void stopR() throws Exception;

  public boolean isStopped();

  public void cancelExecution() throws Exception;

  public boolean isRecoveryEnabled();

  void setRecoveryEnabled(boolean enableRecovery);

  public Process getProcess();
}
