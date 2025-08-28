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
package prerna.util.usertracking;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

public class AnalyticsTrackerHelper {

  private AnalyticsTrackerHelper() {}

  /**
   * Flush a nounstore into a map key-value
   *
   * @param store
   * @param keysToGet
   * @return
   */
  public static Map<String, List<String>> getHashInputs(NounStore store, String[] keysToGet) {
    Map<String, List<String>> keyValues = new HashMap<String, List<String>>();
    for (String key : keysToGet) {
      GenRowStruct grs = store.getNoun(key);
      if (grs == null) {
        continue;
      }
      int size = grs.size();
      List<String> values = new Vector<String>();
      for (int i = 0; i < size; i++) {
        values.add(grs.get(i) + "");
      }
      keyValues.put(key, values);
    }
    return keyValues;
  }
}
