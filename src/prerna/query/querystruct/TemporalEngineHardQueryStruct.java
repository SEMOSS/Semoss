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
package prerna.query.querystruct;

import java.util.Map;

public class TemporalEngineHardQueryStruct extends HardSelectQueryStruct {

  private Map<String, Object> config = null;

  public TemporalEngineHardQueryStruct() {}

  public void setConfig(Map<String, Object> config) {
    this.config = config;
  }

  public Map<String, Object> getConfig() {
    return this.config;
  }

  @Override
  public SelectQueryStruct getNewBaseQueryStruct() {
    TemporalEngineHardQueryStruct newQs = new TemporalEngineHardQueryStruct();
    newQs.setQsType(getQsType());
    newQs.setEngineId(getEngineId());
    // set the physical engine object if appropriate
    newQs.setEngine(getEngine());
    newQs.setConfig(this.config);
    return newQs;
  }
}
