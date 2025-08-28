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
package prerna.reactor.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorFactory;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class HelpJsonReactor extends AbstractReactor {

  /**
   * This reactor allows the user to view the names of all reactors There are no inputs to the
   * reactor
   */
  private static final String RESET_KEY = "reset";

  private static Map<String, Set<String>> helpMap = null;
  private static Map<String, Set<String>> adminHelpMap = null;

  public HelpJsonReactor() {
    this.keysToGet = new String[] {RESET_KEY};
  }

  @Override
  public NounMetadata execute() {
    User user = this.insight.getUser();
    boolean isAdmin = (SecurityAdminUtils.getInstance(user)) != null;
    organizeKeys();
    boolean reset = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[0]));
    if (isAdmin) {
      if (reset || adminHelpMap == null) {
        HelpJsonReactor.adminHelpMap = createHelp(true);
      }
      return new NounMetadata(
          HelpJsonReactor.adminHelpMap, PixelDataType.MAP, PixelOperationType.HELP_JSON);
    } else {
      if (reset || helpMap == null) {
        HelpJsonReactor.helpMap = createHelp(false);
      }
      return new NounMetadata(
          HelpJsonReactor.helpMap, PixelDataType.MAP, PixelOperationType.HELP_JSON);
    }
  }

  /**
   * @param isAdmin
   * @return
   */
  private Map<String, Set<String>> createHelp(boolean isAdmin) {
    Map<String, Set<String>> retMap = new HashMap<>();
    retMap.put(
        "General", nonAdminFormat(new TreeSet(ReactorFactory.reactorHash.keySet()), isAdmin));
    retMap.put("R", nonAdminFormat(new TreeSet(ReactorFactory.rFrameHash.keySet()), isAdmin));
    retMap.put(
        "PYTHON", nonAdminFormat(new TreeSet(ReactorFactory.pandasFrameHash.keySet()), isAdmin));
    retMap.put("H2", nonAdminFormat(new TreeSet(ReactorFactory.h2FrameHash.keySet()), isAdmin));
    retMap.put(
        "NATIVE", nonAdminFormat(new TreeSet(ReactorFactory.nativeFrameHash.keySet()), isAdmin));
    retMap.put(
        "TINKER", nonAdminFormat(new TreeSet(ReactorFactory.tinkerFrameHash.keySet()), isAdmin));
    retMap.put(
        "EXPRESSION", nonAdminFormat(new TreeSet(ReactorFactory.expressionHash.keySet()), isAdmin));
    return retMap;
  }

  private Set<String> nonAdminFormat(TreeSet<String> t, boolean isAdmin) {
    if (isAdmin) {
      return t;
    }
    Iterator<String> iterator = t.iterator();
    while (iterator.hasNext()) {
      if (iterator.next().toString().toLowerCase().startsWith("admin")) {
        iterator.remove();
      }
    }
    return t;
  }
}
