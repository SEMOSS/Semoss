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
package prerna.reactor.browser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class BrowserUtils {

  private static final Logger classLogger = LogManager.getLogger(BrowserUtils.class);

  public static void ensureUserLoggedIn(User user) {
    if (user == null) {
      NounMetadata noun =
          new NounMetadata(
              "User is not logged in. Cannot open URL.",
              PixelDataType.CONST_STRING,
              PixelOperationType.ERROR,
              PixelOperationType.LOGGIN_REQUIRED_ERROR);
      SemossPixelException err = new SemossPixelException(noun);
      err.setContinueThreadOfExecution(false);
      throw err;
    }
  }

  public static boolean anonymousEnabledAndUserAnonymous(User user) {
    return AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous();
  }

  public static String getNonNullString(Map<String, String> keyValue, String key) {
    String res = keyValue.get(key);
    if (res == null) {
      String error = "KeyValue for <" + key + "> cannot be null.";
      throw new IllegalArgumentException(error);
    }
    return res;
  }

  public static int getNonNullInt(Map<String, String> keyValue, String key) {
    String val = getNonNullString(keyValue, key);
    return Integer.parseInt(val);
  }

  public static String mapToJsonString(Map<String, Object> input) {
    ObjectMapper om = new ObjectMapper();
    try {
      return om.writeValueAsString(input);
    } catch (JsonProcessingException e) {
      classLogger.error("Could not parse map with inputs: {}", input.toString());
      throw new IllegalArgumentException("Could not process input and make it json string", e);
    }
  }
}
