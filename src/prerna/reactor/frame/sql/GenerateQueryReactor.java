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
package prerna.reactor.frame.sql;

import java.util.HashMap;
import java.util.Map;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GenerateQueryReactor extends AbstractReactor {

  public GenerateQueryReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.SQL.getKey()};
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata execute() {
    // TODO Auto-generated method stub

    organizeKeys();

    try {
      String sql = keyValue.get(keysToGet[0]);
      String param = keyValue.get(keysToGet[1]);

      GenExpressionWrapper wrapper = this.insight.getSQLWrapper(sql);
      wrapper.fillParameters();
      wrapper.generateQuery(true);

      Map<String, Object> returnMap = new HashMap<String, Object>();
      returnMap.put("query", sql);

      return new NounMetadata(returnMap, PixelDataType.MAP);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      return NounMetadata.getErrorNounMessage(e.getLocalizedMessage());
    }
  }
}
