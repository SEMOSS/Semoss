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

import java.util.HashSet;
import java.util.Set;

public class ProjectCustomReactorCompilator {

  private static Set<String> compiled = new HashSet<>();
  private static Set<String> failed = new HashSet<>();

  private ProjectCustomReactorCompilator() {}

  public static void setCompiled(String projectId) {
    compiled.add(projectId);
    failed.remove(projectId);
  }

  public static void setFailed(String projectId) {
    compiled.remove(projectId);
    failed.add(projectId);
  }

  public static boolean needsCompilation(String projectId) {
    return !compiled.contains(projectId) && !failed.contains(projectId);
  }

  public static boolean isCompiled(String projectId) {
    return compiled.contains(projectId);
  }

  public static boolean isFailed(String projectId) {
    return failed.contains(projectId);
  }

  public static void reset(String projectId) {
    compiled.remove(projectId);
    failed.remove(projectId);
  }
}
