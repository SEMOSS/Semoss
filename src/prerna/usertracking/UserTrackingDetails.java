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
package prerna.usertracking;

// TODO: Once we hit java 17 this should 1000% be a records class
public class UserTrackingDetails {

  private String ipAddr;
  private String ipLat;
  private String ipLong;
  private String ipCountry;
  private String ipState;
  private String ipCity;

  public UserTrackingDetails(
      String ipAddr, String ipLat, String ipLong, String ipCountry, String ipState, String ipCity) {
    this.ipAddr = ipAddr;
    this.ipLat = ipLat;
    this.ipLong = ipLong;
    this.ipCountry = ipCountry;
    this.ipState = ipState;
    this.ipCity = ipCity;
  }

  public String getIpAddr() {
    return ipAddr;
  }

  public void setIpAddr(String ipAddr) {
    this.ipAddr = ipAddr;
  }

  public String getIpLat() {
    return ipLat;
  }

  public void setIpLat(String ipLat) {
    this.ipLat = ipLat;
  }

  public String getIpLong() {
    return ipLong;
  }

  public void setIpLong(String ipLong) {
    this.ipLong = ipLong;
  }

  public String getIpCountry() {
    return ipCountry;
  }

  public void setIpCountry(String ipCountry) {
    this.ipCountry = ipCountry;
  }

  public String getIpState() {
    return ipState;
  }

  public void setIpState(String ipState) {
    this.ipState = ipState;
  }

  public String getIpCity() {
    return ipCity;
  }

  public void setIpCity(String ipCity) {
    this.ipCity = ipCity;
  }

  @Override
  public String toString() {
    return "SessionTrackedDetails [ipAddr="
        + ipAddr
        + ", ipLat="
        + ipLat
        + ", ipLong="
        + ipLong
        + ", ipCountry="
        + ipCountry
        + ", ipState="
        + ipState
        + ", ipCity="
        + ipCity
        + "]";
  }
}
