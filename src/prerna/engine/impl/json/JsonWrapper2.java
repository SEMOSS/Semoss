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
package prerna.engine.impl.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Hashtable;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;

public class JsonWrapper2 extends JsonWrapper {

  private ArrayNode data = null;

  @Override
  public void execute() throws Exception {
    // sorry for the bad way to transport data
    Hashtable output = (Hashtable) engine.execQuery(query);
    this.data = (ArrayNode) output.get("DATA");

    this.headers = (String[]) output.get("HEADERS");
    this.numColumns = this.headers.length;
    this.numRows = (Integer) output.get("COUNT");

    String[] strTypes = (String[]) output.get("TYPES");
    this.types = new SemossDataType[this.numColumns];
    for (int i = 0; i < this.numColumns; i++) {
      this.types[i] = SemossDataType.convertStringToDataType(strTypes[i]);
    }
  }

  @Override
  public IHeadersDataRow next() {
    ArrayNode thisRow = (ArrayNode) data.get(curRow);

    Object[] values = new Object[this.numColumns];
    for (int colIndex = 0; colIndex < this.numColumns; colIndex++) {
      Object retValue = null;
      JsonNode value = thisRow.get(colIndex);
      if (types[colIndex] == SemossDataType.STRING) {
        // check if value is an object if so stringify
        if (value.isArray()) {
          ArrayNode arrayValue = (ArrayNode) value;
          retValue = arrayValue.toString();
        } else if (value.isObject()) {
          ObjectNode objectValue = (ObjectNode) value;
          retValue = objectValue.toString();
        } else {
          retValue = value.asText("");
        }
      } else if (types[colIndex] == SemossDataType.DOUBLE) {
        retValue = value.asDouble();
      } else if (types[colIndex] == SemossDataType.INT) {
        retValue = value.asInt();
      }

      values[colIndex] = retValue;
    }
    this.curRow++;

    IHeadersDataRow retRow = new HeadersDataRow(this.headers, values);
    return retRow;
  }
}
