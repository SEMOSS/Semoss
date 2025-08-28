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
package prerna.reactor.qs.selectors;

import java.util.ArrayList;
import java.util.List;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DateFormatReactor extends SelectReactor {

  @Override
  protected AbstractQueryStruct createQueryStruct() {
    List<IQuerySelector> innerSelectors = new ArrayList<IQuerySelector>();
    GenRowStruct qsInputs = this.getCurRow();
    if (qsInputs == null || qsInputs.isEmpty()) {
      throw new IllegalArgumentException("Must define the filter function for the input");
    }
    int size = qsInputs.size();
    if (size < 2) {
      throw new IllegalArgumentException(
          "Must pass in at least 2 parameters, a column and the format string");
    }

    for (int selectIndex = 0; selectIndex < size; selectIndex++) {
      NounMetadata input = qsInputs.getNoun(selectIndex);
      IQuerySelector innerSelector = getSelector(input);
      innerSelectors.add(innerSelector);
    }
    qs.addSelector(genFunctionSelector(QueryFunctionHelper.DATE_FORMAT, innerSelectors));
    return qs;
  }
}
