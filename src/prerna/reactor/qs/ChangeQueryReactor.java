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
package prerna.reactor.qs;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;

public class ChangeQueryReactor extends AbstractQueryStructReactor {

  // replaces part of the query - which ever the uses wishes to replace

  public ChangeQueryReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.QUERY_PART.getKey()};
  }

  protected AbstractQueryStruct createQueryStruct() {
    // if the query struct is empty fill this into the insight
    List<Object> mapOptions = this.curRow.getValuesOfType(PixelDataType.MAP);
    prerna.query.querystruct.SelectQueryStruct sqs = null;
    String panelId = insight.getLastPanelId();
    // try to see if this qs has a frame -

    // usually there should be just one
    // and that does it
    if (mapOptions != null && !mapOptions.isEmpty()) {
      // if it is null, i guess we just clear the map values
      // this.qs.setPragmap(mapOptions);
      Map input = (Map) mapOptions.get(0);
      if (input.containsKey("id")) panelId = (String) input.get("id");

      if (this.qs != null && !this.qs.getSelectors().isEmpty())
        sqs = (SelectQueryStruct) qs; // this ensures I can even use it enroute
      else sqs = insight.getLastQS(panelId);

      Iterator keyIterator = input.keySet().iterator();

      while (keyIterator.hasNext()) {
        String thisKey = (String) keyIterator.next();
        String value = (String) input.get(thisKey);
        thisKey = thisKey.toUpperCase();
        SelectQueryStruct.Query_Part part = null;

        switch (thisKey) {
          case "SELECT":
            part = SelectQueryStruct.Query_Part.SELECT;
            break;
          case "ORDER":
            part = SelectQueryStruct.Query_Part.SORT;
            break;
          case "GROUP":
            part = SelectQueryStruct.Query_Part.GROUP;
            break;
          case "AGGREGATE":
            part = SelectQueryStruct.Query_Part.AGGREGATE;
            break;
          case "FILTER":
            part = SelectQueryStruct.Query_Part.FILTER;
            break;
          case "QUERY":
            part = SelectQueryStruct.Query_Part.QUERY;
            break;
        }

        // if(qs != null)
        // ((SelectQueryStruct)qs).setPart(part, value);
        // else
        // this.insight.setPart(part, value);
        sqs.setPart(part, value);
      }
      // do it here
      if (input.size() == 0) {
        sqs.getParts().clear();
        insight.setLastQS(sqs, panelId);
      }
    }
    // need to do this to trigger the iterator
    /*
    reactor.In();
    reactor.setInsight(this.insight);
    reactor.setNounStore(this.store);
    return reactor.execute();
    */

    // this is needed because the collect block looks for the last key to get the
    // task options
    // forget thread safety - it is for birds
    insight.setLastPanelId(panelId);

    if (sqs != null) {
      sqs.setLimit(-1);
    }

    return sqs;
  }
}
