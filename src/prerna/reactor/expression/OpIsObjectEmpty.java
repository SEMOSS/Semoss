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
package prerna.reactor.expression;

import java.util.Collection;
import java.util.Map;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpIsObjectEmpty extends OpBasic {

  public OpIsObjectEmpty() {
    this.keysToGet = new String[] {ReactorKeysEnum.VALUES.getKey()};
  }

  @Override
  protected NounMetadata evaluate(Object[] values) {
    boolean isEmpty = true;
    if (values != null && values.length > 0) {
      Object input = values[0];
      if (input instanceof Collection) {
        isEmpty = ((Collection) input).isEmpty();
      } else if (input instanceof Map) {
        isEmpty = ((Map) input).isEmpty();
      }
    }
    return new NounMetadata(isEmpty, PixelDataType.BOOLEAN);
  }

  @Override
  public String getReturnType() {
    return "boolean";
  }
}
