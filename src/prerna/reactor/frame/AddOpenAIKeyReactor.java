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
package prerna.reactor.frame;

import java.util.HashMap;
import java.util.Map;
import prerna.ds.py.PyTranslator;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddOpenAIKeyReactor extends AbstractReactor {

  // if you ask without key
  // you will get if it is defined or not
  // if not you can add it
  String OPENAI_DEFINED = "openai_defined";

  public AddOpenAIKeyReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.API_KEY.getKey()};
    this.keyRequired = new int[] {0};
  }

  @Override
  public NounMetadata execute() {
    // do we need a way to check the library is installed?

    organizeKeys();
    PyTranslator pt = this.insight.getPyTranslator();
    if (keyValue.containsKey(keysToGet[0])) {
      // set the key
      String api_key = keyValue.get(keysToGet[0]);
      pt.runEmptyPy("import openai", "openai.api_key='" + api_key + "'", OPENAI_DEFINED + "= True");
    }

    boolean output = (Boolean) pt.runDirectPy("'" + OPENAI_DEFINED + "' in globals()");

    Map<String, Object> outMap = new HashMap<>();
    outMap.put(OPENAI_DEFINED, output);
    return new NounMetadata(outMap, PixelDataType.MAP);
  }
}
