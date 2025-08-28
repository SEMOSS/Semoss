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
package prerna.reactor.qs.filter;

import java.util.List;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;

public class FilterReactor extends AbstractQueryStructReactor {

  public FilterReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.FILTERS.getKey()};
  }

  protected AbstractQueryStruct createQueryStruct() {
    List<Object> filters = this.curRow.getValuesOfType(PixelDataType.FILTER);
    for (int i = 0; i < filters.size(); i++) {
      IQueryFilter nextFilter = (IQueryFilter) filters.get(i);
      if (nextFilter != null) {
        if (nextFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
          if (isValidFilter((SimpleQueryFilter) nextFilter)) {
            qs.addExplicitFilter(nextFilter);
          }
        } else {
          qs.addExplicitFilter(nextFilter);
        }
      }
    }
    return qs;
  }

  protected boolean isValidFilter(SimpleQueryFilter filter) {
    SimpleQueryFilter.FILTER_TYPE filterType = filter.getSimpleFilterType();
    if (filterType == SimpleQueryFilter.FILTER_TYPE.COL_TO_VALUES) {
      // make sure right side has values
      Object rightSide = filter.getRComparison().getValue();
      if (rightSide instanceof List) {
        return ((List) rightSide).size() > 0;
      }
    } else if (filterType == SimpleQueryFilter.FILTER_TYPE.VALUES_TO_COL) {
      // make sure left side has values
      Object leftSide = filter.getLComparison().getValue();
      if (leftSide instanceof List) {
        return ((List) leftSide).size() > 0;
      }
    }
    // meh, just return true
    return true;
  }
}
