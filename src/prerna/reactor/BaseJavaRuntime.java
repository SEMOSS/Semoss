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
package prerna.reactor;

import java.util.HashMap;
import java.util.Map;

public class BaseJavaRuntime {

  public Map<String, Object> variables = new HashMap<>();

  /**
   * Method that return the evaluation of the signature
   *
   * @return
   */
  public void execute() {}

  /** Method that will run any updates to the base model */
  public void update() {}

  public Map<String, Object> getVariables() {
    return this.variables;
  }

  public void a(String var) {
    variables.put(var, 1);
  }

  public void a(String var, double value) {
    variables.put(var, value);
  }

  public void a(String var, int value) {
    variables.put(var, value);
  }

  public void a(String var, String value) {
    variables.put(var, value);
  }

  public void a(String var, boolean value) {
    variables.put(var, value);
  }

  public boolean compareString(String lString, String comparator, String rString) {
    if (comparator.equals("==")) {
      return lString.equals(rString);
    } else {
      return !lString.equals(rString);
    }
  }

  public boolean compareString(double lValue, String comparator, String rString) {
    return false;
  }

  public boolean compareString(int lValue, String comparator, String rString) {
    return false;
  }
}
