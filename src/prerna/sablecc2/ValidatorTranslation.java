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
package prerna.sablecc2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import prerna.reactor.ReactorFactory;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AOperation;

public class ValidatorTranslation extends DepthFirstAdapter {

  private Map<Boolean, Set<String>> implementedReactorChecks;

  public ValidatorTranslation() {
    implementedReactorChecks = new HashMap<>(2);
    implementedReactorChecks.put(true, new HashSet<>());
    implementedReactorChecks.put(false, new HashSet<>());
  }

  public Set<String> getUnimplementedReactors() {
    return implementedReactorChecks.get(false);
  }

  @Override
  public void inAOperation(AOperation node) {
    String reactorId = node.getId().toString().trim();
    boolean isImplemented = ReactorFactory.hasReactor(reactorId);
    implementedReactorChecks.get(isImplemented).add(reactorId);
  }
}
