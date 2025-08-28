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

import java.util.ArrayList;
import java.util.List;

public class InGenExpression extends GenExpression {

  // false = in
  // true = not in
  private boolean isNot = false;

  // sets the condition
  public List<GenExpression> inList = new ArrayList<GenExpression>();

  public void setIsNot(boolean negate) {
    this.isNot = negate;
  }

  public boolean isNot() {
    return this.isNot;
  }
}
