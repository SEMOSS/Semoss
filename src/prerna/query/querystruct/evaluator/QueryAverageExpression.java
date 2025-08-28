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
package prerna.query.querystruct.evaluator;

public class QueryAverageExpression implements IQueryStructExpression {

  private double sum = 0.0;
  private int count = 0;

  @Override
  public void processData(Object obj) {
    if (obj instanceof Number) {
      double newValue = ((Number) obj).doubleValue();
      this.sum += newValue;
      this.count++;
    }
  }

  @Override
  public Object getOutput() {
    if (this.count == 0) {
      return 0;
    }
    return this.sum / this.count;
  }
}
