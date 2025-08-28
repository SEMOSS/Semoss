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
package prerna.rpa.config;

public class JobConfigKeys {

  private JobConfigKeys() {
    throw new IllegalStateException("Constants class");
  }

  // Job metadata
  // All jobs may have these keys
  // However, they are are not always required
  public static final String JOB_ID = "-jobId";
  public static final String JOB_NAME = "-jobName";
  public static final String JOB_GROUP = "-jobGroup";
  public static final String JOB_CLASS_NAME = "-jobClass";
  public static final String JOB_CRON_EXPRESSION = "-jobCronExpression";
  public static final String JOB_CRON_TIMEZONE = "-jobCronTimeZone";
  public static final String TRIGGER_ON_LOAD = "-jobTriggerOnLoad";
  public static final String ACTIVE = "-active";
  public static final String USER_ACCESS = "-userAccess";
  public static final String PIXEL = "pixel";
  public static final String PIXEL_PARAMETERS = "pixelParameters";
  public static final String UI_STATE = "uiState"; // Cannot have the dash due to FE

  // execution side
  public static final String EXEC_ID = "-execId"; // Cannot have the dash due to FE
}
