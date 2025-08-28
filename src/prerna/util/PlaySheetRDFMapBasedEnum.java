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
package prerna.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Enables a variable to be a set of predefined constants. This class defines constants for all
 * types of playsheets, which includes their names, source file location, and the hint for a SPARQL
 * query associated with each.
 */
public class PlaySheetRDFMapBasedEnum {

  static Map<String, Map<String, Object>>
      masterObject; // this is rdf map key -> object containing sheet name, class name, hint
  private static PlaySheetRDFMapBasedEnum instance;

  // for storing in master object
  //	private final static String sheetName = "sheetName";
  private static final String sheetClass = "sheetClass";
  private static final String sheetHint = "sheetHint";
  private static final String sheetCustom = "sheetCustom";
  private static final String sheetID = "sheetID";

  // for getting off of rdf map
  private static final String MAP_HINT = "_HINT";
  private static final String MAP_CUSTOM = "_CUSTOM";
  private static final String MAP_CLASS = "";

  //	private final static String MAP_NAME = "_NAME";

  public static PlaySheetRDFMapBasedEnum getInstance() {
    if (instance == null) {
      instance = new PlaySheetRDFMapBasedEnum();
    }
    return instance;
  }

  private PlaySheetRDFMapBasedEnum() {
    // protected
  }

  public void setData(String[] psIdsToLoad, Properties props) {
    masterObject = new LinkedHashMap<String, Map<String, Object>>();
    // for each playsheet listed on the map
    // grab as much information as we can about it
    // save in our master object so we can reference
    for (String id : psIdsToLoad) {
      Map<String, Object> psObj = new HashMap<String, Object>();
      psObj.put(sheetID, id);

      String psHintKey = id + MAP_HINT;
      String psClassKey = id + MAP_CLASS;
      String psCustomKey = id + MAP_CUSTOM;
      //			String psNameKey = id + MAP_NAME;

      if (props.containsKey(psHintKey)) {
        psObj.put(sheetHint, props.getProperty(psHintKey));
      } else {
        psObj.put(sheetHint, "No hint defined");
      }
      if (props.containsKey(psClassKey)) {
        psObj.put(sheetClass, props.getProperty(psClassKey));
      } else {
        psObj.put(sheetClass, "No class defined");
      }
      if (props.containsKey(psCustomKey)) {
        psObj.put(sheetCustom, Boolean.parseBoolean(props.getProperty(psCustomKey)));
      } else {
        psObj.put(sheetCustom, true);
      }
      //			if(props.containsKey(psNameKey)){
      //				psObj.put(sheetName, props.getProperty(psNameKey));
      //			}
      //			else{
      //				psObj.put(sheetName, "No name defined");
      //			}
      masterObject.put(id, psObj);
    }
  }

  public String getSheetClass(String psId) {
    return (String) masterObject.get(psId).get(sheetClass);
  }

  public static String getSheetName(String psId) {
    return (String) masterObject.get(psId).get(sheetID);
  }

  public String getSheetHint(String psId) {
    return (String) masterObject.get(psId).get(sheetHint);
  }

  public static List<String> getAllSheetNames() {
    ArrayList<String> list = new ArrayList<String>();
    for (Map<String, Object> e : masterObject.values()) {
      list.add((String) e.get(sheetID));
    }
    return list;
  }

  public static ArrayList<String> getAllSheetClasses() {
    ArrayList<String> list = new ArrayList<String>();
    for (Map<String, Object> e : masterObject.values()) {
      list.add((String) e.get(sheetClass));
    }
    return list;
  }

  public static String getClassFromName(String checkName) {
    String match = "";
    for (Map<String, Object> e : masterObject.values()) {
      if (e.get(sheetID).equals(checkName)) {
        match = (String) e.get(sheetClass);
        break;
      }
    }
    return match;
  }

  public static String getHintFromName(String checkName) {
    String match = "";
    for (Map<String, Object> e : masterObject.values()) {
      if (e.get(sheetID).equals(checkName)) {
        match = (String) e.get(sheetHint);
        break;
      }
    }
    return match;
  }

  public static String getNameFromClass(String checkClass) {
    String match = "";
    for (Map<String, Object> e : masterObject.values()) {
      if (e.get(sheetClass).equals(checkClass)) {
        match = (String) e.get(sheetID);
        break;
      }
    }
    return match;
  }

  public static String getIdFromClass(String checkClass) {
    String match = "";
    for (Map<String, Object> e : masterObject.values()) {
      if (e.get(sheetClass).equals(checkClass)) {
        match = (String) e.get(sheetID);
        break;
      }
    }
    return match;
  }

  public static List<String> getCustomSheetNames() {
    ArrayList<String> list = new ArrayList<String>();
    for (Map<String, Object> e : masterObject.values()) {
      if ((Boolean) e.get(sheetCustom)) {
        list.add((String) e.get(sheetID));
      }
    }
    return list;
  }
}
