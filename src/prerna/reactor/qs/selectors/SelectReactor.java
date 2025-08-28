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
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskUtility;

public class SelectReactor extends AbstractQueryStructReactor {

  public SelectReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.COLUMNS.getKey()};
  }

  protected AbstractQueryStruct createQueryStruct() {
    GenRowStruct qsInputs = this.getCurRow();
    if (qsInputs != null && !qsInputs.isEmpty()) {
      List<IQuerySelector> selectors = new Vector<IQuerySelector>();
      for (int selectIndex = 0; selectIndex < qsInputs.size(); selectIndex++) {
        NounMetadata input = qsInputs.getNoun(selectIndex);
        IQuerySelector selector = getSelector(input);
        if (selector != null) {
          selectors.add(selector);
        }
      }
      setAlias(selectors, this.selectorAlias, 0);
      qs.mergeSelectors(selectors);
    }

    if (qs.getPragmap() == null) qs.setPragmap(new java.util.HashMap());

    if (insight.getPragmap() != null) qs.getPragmap().putAll(insight.getPragmap());

    return qs;
  }

  protected IQuerySelector getSelector(NounMetadata input) {
    PixelDataType nounType = input.getNounType();
    if (nounType == PixelDataType.QUERY_STRUCT) {
      // remember, if it is an embedded selector
      // we return a full QueryStruct even if it has just one selector
      // inside of it
      SelectQueryStruct qs = (SelectQueryStruct) input.getValue();
      List<IQuerySelector> selectors = qs.getSelectors();
      if (selectors.isEmpty()) {
        // umm... merge the other QS stuff
        qs.merge(qs);
        return null;
      }
      return selectors.get(0);
    } else if (nounType == PixelDataType.COLUMN) {
      return (IQuerySelector) input.getValue();
    } else if (nounType == PixelDataType.FORMATTED_DATA_SET || nounType == PixelDataType.TASK) {
      Object value = input.getValue();
      NounMetadata formatData = TaskUtility.getTaskDataScalarElement(value);
      if (formatData == null) {
        throw new IllegalArgumentException("Can only handle query data that is a scalar input");
      } else {
        Object newValue = formatData.getValue();
        QueryConstantSelector cSelect = new QueryConstantSelector();
        cSelect.setConstant(newValue);
        return cSelect;
      }
    } else {
      // we have a constant...
      QueryConstantSelector cSelect = new QueryConstantSelector();
      cSelect.setConstant(input.getValue());
      return cSelect;
    }
  }

  protected QueryFunctionSelector genFunctionSelector(
      String functionName, IQuerySelector innerSelector) {
    return genFunctionSelector(functionName, innerSelector, false);
  }

  protected QueryFunctionSelector genFunctionSelector(
      String functionName, IQuerySelector innerSelector, boolean isDistinct) {
    QueryFunctionSelector newSelector = new QueryFunctionSelector();
    newSelector.addInnerSelector(innerSelector);
    newSelector.setFunction(functionName);
    newSelector.setDistinct(isDistinct);
    return newSelector;
  }

  protected QueryFunctionSelector genFunctionSelector(
      String functionName, List<IQuerySelector> innerSelectors) {
    return genFunctionSelector(functionName, innerSelectors, false);
  }

  protected QueryFunctionSelector genFunctionSelector(
      String functionName, List<IQuerySelector> innerSelectors, boolean isDistinct) {
    QueryFunctionSelector newSelector = new QueryFunctionSelector();
    newSelector.setInnerSelector(innerSelectors);
    newSelector.setFunction(functionName);
    newSelector.setDistinct(isDistinct);
    return newSelector;
  }
}
