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
package prerna.reactor.json.processor;

import java.util.Hashtable;
import java.util.List;
import java.util.Vector;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ProfileProcessor extends AbstractReactor {

  @Override
  public NounMetadata execute() {
    System.out.println("Process a profile");
    List<String> sqlQueries = new Vector<String>();
    if (this.curRow != null && !this.curRow.isEmpty()) {
      for (int i = 0; i < this.curRow.size(); i++) {
        NounMetadata val = this.curRow.getNoun(i);
        if (val.getNounType() == PixelDataType.CONST_STRING) {
          sqlQueries.add(val.getValue().toString());
        } else if (val.getNounType() == PixelDataType.VECTOR) {
          sqlQueries.addAll((List) val.getValue());
        }
      }
    }

    Hashtable<String, Object> data = this.store.getDataHash();
    //		System.out.println(data);

    StringBuilder sb = new StringBuilder();
    sb.append("PROFILE INSERT!!!");
    sqlQueries.add(sb.toString());

    return new NounMetadata(sqlQueries, PixelDataType.VECTOR);
  }
}
