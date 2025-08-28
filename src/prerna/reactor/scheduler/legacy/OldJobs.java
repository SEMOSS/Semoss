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
package prerna.reactor.scheduler.legacy;

import com.google.gson.annotations.SerializedName;

@Deprecated
public class OldJobs {
  @SerializedName("-jobName")
  private String jobName;

  @SerializedName("-jobGroup")
  private String jobGroup;

  @SerializedName("-jobCronExpression")
  private String jobCronExpression;

  @SerializedName("-jobClass")
  private String jobClass;

  @SerializedName("-active")
  private String active;

  @SerializedName("-userAccess")
  private String userAccess;

  @SerializedName("-jobTriggerOnLoad")
  private String jobTriggerOnLoad;

  private String pixel;
  private String parameters;
  private String hidden;

  public String getJobTriggerOnLoad() {
    return jobTriggerOnLoad;
  }

  public void setJobTriggerOnLoad(String jobTriggerOnLoad) {
    this.jobTriggerOnLoad = jobTriggerOnLoad;
  }

  public String getParameters() {
    return parameters;
  }

  public void setParameters(String parameters) {
    this.parameters = parameters;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getJobGroup() {
    return jobGroup;
  }

  public void setJobGroup(String jobGroup) {
    this.jobGroup = jobGroup;
  }

  public String getJobCronExpression() {
    return jobCronExpression;
  }

  public void setJobCronExpression(String jobCronExpression) {
    this.jobCronExpression = jobCronExpression;
  }

  public String getJobClass() {
    return jobClass;
  }

  public void setJobClass(String jobClass) {
    this.jobClass = jobClass;
  }

  public String getPixel() {
    return pixel;
  }

  public void setPixel(String pixel) {
    this.pixel = pixel;
  }

  public String getActive() {
    return active;
  }

  public void setActive(String active) {
    this.active = active;
  }

  public String getUserAccess() {
    return userAccess;
  }

  public void setUserAccess(String userAccess) {
    this.userAccess = userAccess;
  }

  public String getHidden() {
    return hidden;
  }

  public void setHidden(String hidden) {
    this.hidden = hidden;
  }
}
