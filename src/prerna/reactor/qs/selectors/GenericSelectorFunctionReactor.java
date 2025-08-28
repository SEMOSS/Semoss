/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.qs.selectors;

import java.util.List;
import java.util.Set;
import java.util.Vector;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GenericSelectorFunctionReactor extends SelectReactor {

  private String function = null;

  @Override
  protected AbstractQueryStruct createQueryStruct() {
    // try to create the function selector
    List<IQuerySelector> innerSelectors = new Vector<IQuerySelector>();
    GenRowStruct qsInputs = this.getCurRow();
    if (qsInputs != null && !qsInputs.isEmpty()) {
      for (int selectIndex = 0; selectIndex < qsInputs.size(); selectIndex++) {
        NounMetadata input = qsInputs.getNoun(selectIndex);
        IQuerySelector innerSelector = getSelector(input);
        innerSelectors.add(innerSelector);
      }
    }

    QueryFunctionSelector functionSelector = genFunctionSelector(function, innerSelectors);
    qs.addSelector(functionSelector);
    Set<String> keys = this.store.getNounKeys();
    for (String key : keys) {
      if (key.equals("all")) {
        continue;
      } else if (key.equals("sDataType")) {
        String dataType = this.store.getNoun(key).get(0).toString();
        functionSelector.setDataType(dataType);
        continue;
      }
      GenRowStruct grs = this.store.getNoun(key);
      int num = grs.size();
      Object[] additionalParams = new Object[num + 1];
      additionalParams[0] = key;
      for (int i = 0; i < num; i++) {
        additionalParams[i + 1] = grs.get(i);
      }

      functionSelector.addAdditionalParam(additionalParams);
    }

    return qs;
  }

  public void setFunction(String function) {
    this.function = function;
  }
}
