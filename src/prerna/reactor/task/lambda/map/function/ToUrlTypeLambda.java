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
package prerna.reactor.task.lambda.map.function;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.reactor.task.lambda.map.AbstractMapLambda;

public class ToUrlTypeLambda extends AbstractMapLambda {

  private int numCols;
  private List<Integer> colIndices;

  @Override
  public IHeadersDataRow process(IHeadersDataRow row) {
    String[] headers = row.getHeaders();
    Object[] values = row.getValues();
    for (int i = 0; i < numCols; i++) {
      int indexToGet = colIndices.get(i).intValue();
      // try to convert it
      values[indexToGet] = values[indexToGet].toString();
    }

    return new HeadersDataRow(headers, values);
  }

  @Override
  public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
    this.headerInfo = headerInfo;

    // figure out which indices are those we want to convert to a double
    List<Integer> indices = new ArrayList<Integer>();
    int totalCols = headerInfo.size();
    int inputCols = columns.size();

    // this modifies the header info map by reference
    NEXT_COLUMN:
    for (int i = 0; i < inputCols; i++) {
      String headerToConvert = columns.get(i);
      for (int j = 0; j < totalCols; j++) {
        Map<String, Object> headerMap = headerInfo.get(j);
        String alias = headerMap.get("alias").toString();
        if (alias.equals(headerToConvert)) {
          // add the index to convert
          // modify the type to double
          indices.add(new Integer(j));
          headerMap.put("type", "URL");
          continue NEXT_COLUMN;
        }
      }
    }

    this.colIndices = indices;
    this.numCols = colIndices.size();
  }
}
