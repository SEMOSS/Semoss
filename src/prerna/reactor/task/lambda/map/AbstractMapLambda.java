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
package prerna.reactor.task.lambda.map;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.auth.User;

public abstract class AbstractMapLambda implements IMapLambda {

  protected Map params = new HashMap();
  protected List<Map<String, Object>> headerInfo;
  protected User user;

  @Override
  public List<Map<String, Object>> getModifiedHeaderInfo() {
    return this.headerInfo;
  }

  @Override
  public void setUser(User user) {
    this.user = user;
  }

  @Override
  public void setParams(Map params) {
    this.params = params;
  }
}
