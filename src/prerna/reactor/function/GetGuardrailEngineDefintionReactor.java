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
package prerna.reactor.function;

public class GetGuardrailEngineDefintionReactor extends GetFunctionEngineDefintionReactor {

  /*
   * Just a convenience method
   * Works the same as the function engine
   * Since guardrail engine is a function engine as well
   */

  @Override
  String getUnableToAccessError(String engineId) {
    return "Guardrail Engine "
        + engineId
        + " does not exist or user does not have access to this guardrail";
  }
}
