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
package prerna.reactor.expression.filter;

import prerna.reactor.expression.OpBasic;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpOr extends OpBasic {

  public OpOr() {
    this.keysToGet = new String[] {ReactorKeysEnum.VALUES.getKey()};
  }

  @Override
  protected NounMetadata evaluate(Object[] values) {
    boolean result = eval(values);
    return new NounMetadata(result, PixelDataType.BOOLEAN);
  }

  public boolean eval(Object... values) {
    boolean result = false;
    for (Object booleanValue : values) {
      // need only 1 value to be true
      // in order to return true
      if ((boolean) booleanValue) {
        result = true;
        break;
      }
    }
    return result;
  }

  @Override
  public String getReturnType() {
    // TODO Auto-generated method stub
    return "boolean";
  }
}
