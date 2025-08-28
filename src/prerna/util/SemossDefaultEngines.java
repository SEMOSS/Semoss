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
package prerna.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SemossDefaultEngines {

  private static final List<String> IGNORE_DATABASE_OWL = new ArrayList<>();

  static {
    IGNORE_DATABASE_OWL.add(Constants.LOCAL_MASTER_DB);
    IGNORE_DATABASE_OWL.add(Constants.SECURITY_DB);
    IGNORE_DATABASE_OWL.add(Constants.THEMING_DB);
    IGNORE_DATABASE_OWL.add(Constants.SCHEDULER_DB);
    IGNORE_DATABASE_OWL.add(Constants.USER_TRACKING_DB);
  }

  private static final List<String> DATABASE_GENERATED_OWL = new ArrayList<>();

  static {
    DATABASE_GENERATED_OWL.addAll(IGNORE_DATABASE_OWL);
    DATABASE_GENERATED_OWL.add(Constants.MODEL_INFERENCE_LOGS_DB);
    DATABASE_GENERATED_OWL.add(Constants.PROMPT_DB);
  }

  private static final List<String> DATABASE_IGNORE_LOCALMASTER = new ArrayList<>();

  static {
    DATABASE_IGNORE_LOCALMASTER.add(Constants.LOCAL_MASTER_DB);
    DATABASE_IGNORE_LOCALMASTER.add(Constants.SECURITY_DB);
    DATABASE_IGNORE_LOCALMASTER.add(Constants.SCHEDULER_DB);
    DATABASE_IGNORE_LOCALMASTER.add(Constants.THEMING_DB);
    DATABASE_IGNORE_LOCALMASTER.add(Constants.USER_TRACKING_DB);
  }

  private static final List<String> DATABASE_IGNORE_SECURITY = new ArrayList<>();

  static {
    DATABASE_IGNORE_SECURITY.add(Constants.LOCAL_MASTER_DB);
    DATABASE_IGNORE_SECURITY.add(Constants.SECURITY_DB);
    DATABASE_IGNORE_SECURITY.add(Constants.SCHEDULER_DB);
    DATABASE_IGNORE_SECURITY.add(Constants.THEMING_DB);
    DATABASE_IGNORE_SECURITY.add(Constants.USER_TRACKING_DB);
  }

  public static List<String> getIgnoreDatabaseOwlList() {
    return Collections.unmodifiableList(IGNORE_DATABASE_OWL);
  }

  public static List<String> getDatabasesWithGeneratedOwl() {
    return Collections.unmodifiableList(DATABASE_GENERATED_OWL);
  }

  public static List<String> getDatabaseIgnoreLocalMaster() {
    return Collections.unmodifiableList(DATABASE_IGNORE_LOCALMASTER);
  }

  public static List<String> getDatabaseIgnoreSecurity() {
    return Collections.unmodifiableList(DATABASE_IGNORE_SECURITY);
  }

  /**
   * Check if a string starts with any value within a collection
   *
   * @param strValue
   * @param collection
   * @return
   */
  public static boolean valueStartsWith(String strValue, Collection<String> collection) {
    for (String c : collection) {
      if (strValue.startsWith(c)) {
        return true;
      }
    }
    return false;
  }
}
