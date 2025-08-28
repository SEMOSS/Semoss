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

import org.rosuda.REngine.Rserve.RConnection;

public class RserveConnectionMeta {

  private final String host;
  private final int port;
  private RConnection rcon;
  private volatile boolean isActive = false;
  private volatile Process process = null;

  public RserveConnectionMeta(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public RConnection getRcon() {
    return rcon;
  }

  public void setRcon(RConnection rcon) {
    this.rcon = rcon;
  }

  public boolean isActive() {
    return isActive;
  }

  public void setActive(boolean isActive) {
    this.isActive = isActive;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!RserveConnectionMeta.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    RserveConnectionMeta other = (RserveConnectionMeta) obj;
    if (other.getHost().equals(this.host) && other.getPort() == this.port) {
      return true;
    }
    return false;
  }

  public Process getProcess() {
    return process;
  }

  public void setProcess(Process process) {
    this.process = process;
  }
}
