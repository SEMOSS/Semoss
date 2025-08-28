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

import java.util.List;
import java.util.Vector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpList extends OpBasic {

  public OpList() {
    this.operation = "list";
    this.keysToGet = new String[] {ReactorKeysEnum.ARRAY.getKey()};
  }

  @Override
  protected NounMetadata evaluate(Object[] values) {
    List<Object> list = new Vector<>(values.length);
    for (Object v : values) {
      list.add(v);
    }
    NounMetadata noun = new NounMetadata(list, PixelDataType.VECTOR);
    return noun;
  }

  @Override
  public String getReturnType() {
    return "List";
  }
}
