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
package prerna.query.querystruct.filters;

public class BooleanValMetadata {

  private enum BOOLEAN_TYPE {
    FRAME,
    PANEL
  };

  private BOOLEAN_TYPE type = null;
  private String name = null;
  private boolean filterVal = false;

  private BooleanValMetadata() {}

  public static BooleanValMetadata getFrameVal() {
    BooleanValMetadata map = new BooleanValMetadata();
    map.type = BOOLEAN_TYPE.FRAME;
    return map;
  }

  public static BooleanValMetadata getPanelVal() {
    BooleanValMetadata map = new BooleanValMetadata();
    map.type = BOOLEAN_TYPE.PANEL;
    return map;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setFilterVal(boolean filterVal) {
    this.filterVal = filterVal;
  }

  public String getName() {
    return this.name;
  }

  public boolean getFilterVal() {
    return this.filterVal;
  }

  public String getTypeString() {
    return this.type + "";
  }
}
