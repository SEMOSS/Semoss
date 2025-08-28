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
import java.util.Vector;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class IfReactor extends SelectReactor {

  // if else reactor typically has three building blocks into it
  // all of are query selectors
  // condition -  - this could be a query selector - mandatory - specifies what is the condition
  // under which this is valid
  // then - the precedent - what happens when the condition is true - mandatory again
  // else - the antecedent - what happens if the condition is invalid. PLease note that these
  // themselves could be other if reactors
  // the parent of if else is always a SelectReactor

  public IfReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.COLUMNS.getKey()};
  }

  protected AbstractQueryStruct createQueryStruct() {
    // first one is the condition
    // second one is the precedent
    // third one is the antecedent
    QueryIfSelector qis = new QueryIfSelector();
    qis.setPixelString(this.originalSignature);

    GenRowStruct qsInputs = this.getCurRow();
    if (qsInputs != null && !qsInputs.isEmpty()) {
      List<IQuerySelector> selectors = new Vector<IQuerySelector>();
      // there should be three here
      NounMetadata input = qsInputs.getNoun(0);
      IQuerySelector conditionSelector = getSelector(input);
      if (conditionSelector instanceof QueryConstantSelector) {
        IQueryFilter filter =
            (IQueryFilter) ((QueryConstantSelector) conditionSelector).getConstant();
        qis.setCondition(filter);
      }
      NounMetadata input2 = qsInputs.getNoun(1);
      IQuerySelector precedent = getSelector(input2);
      qis.setPrecedent(precedent);

      NounMetadata input3 = qsInputs.getNoun(2);
      if (input3 != null) {
        IQuerySelector antecedent = getSelector(input3);
        qis.setAntecedent(antecedent);
      }
      selectors.add(qis);
      setAlias(selectors, this.selectorAlias, 0);
      qs.mergeSelectors(selectors);
    }

    if (qs.getPragmap() == null) {
      qs.setPragmap(new java.util.HashMap());
    }

    if (insight.getPragmap() != null) {
      qs.getPragmap().putAll(insight.getPragmap());
    }

    // convert this into one single query struct and then return it
    // first one is a filter query struct

    return qs;
  }
}
