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
package prerna.query.interpreters;

import java.util.List;
import java.util.Vector;
import org.apache.logging.log4j.Logger;
import prerna.engine.api.IDatabaseEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class JsonInterpreter implements IQueryInterpreter {

  AbstractQueryStruct qs = null;
  StringBuffer selectors = null;
  StringBuffer filters = null;
  // for fda this is a +
  public String separator = "&";
  IDatabaseEngine engine = null;

  // equalizer
  // for FDA this is a :
  public String equal = "=";

  public JsonInterpreter(IDatabaseEngine engine) {
    this.engine = engine;
  }

  @Override
  public String composeQuery() {
    addSelectors();
    addFilters();

    String retString = "";
    if (selectors != null) retString = selectors.toString();

    if (filters != null) retString = selectors.toString() + "@@@" + filters.toString();

    return retString;
  }

  @Override
  public void setQueryStruct(AbstractQueryStruct qs) {
    this.qs = qs;
  }

  @Override
  public void setDistinct(boolean isDistinct) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isDistinct() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void setLogger(Logger logger) {
    // TODO Auto-generated method stub

  }

  /* Loops through the selectors defined in the QS to add them to the selector string
   * and considers if the table should be added to the from string
   */
  public void addSelectors() {
    List<IQuerySelector> selectorData = qs.getSelectors();
    for (IQuerySelector selector : selectorData) {
      addSelector(selector);
    }
  }

  private void addSelector(IQuerySelector selector) {
    String alias = selector.getAlias();

    String pathPattern = ((QueryColumnSelector) selector).getColumn();

    // this is basically the same thing
    // the path pattern sits on the prop file
    if (alias.equalsIgnoreCase(pathPattern)) {
      if (selectors == null) selectors = new StringBuffer(pathPattern);
      else selectors = selectors.append(";").append(pathPattern);
    }

    // alias=pathPattern

    else {
      String totalString = alias + "=" + pathPattern;
      if (selectors == null) selectors = new StringBuffer(totalString);
      else selectors = selectors.append(";").append(totalString);
    }
  }

  public void addFilters() {
    List<IQueryFilter> filters = qs.getCombinedFilters().getFilters();
    for (IQueryFilter filter : filters) {
      StringBuilder filterSyntax = processFilter(filter);
      if (filterSyntax != null) {
        if (this.filters == null) {
          this.filters = new StringBuffer(filterSyntax);
        } else {
          this.filters = this.filters.append(";").append(filterSyntax);
        }
      }
    }
  }

  private StringBuilder processFilter(IQueryFilter filter) {
    IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
    if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE)
      return processSimpleQueryFilter((SimpleQueryFilter) filter);
    return null;
  }

  private StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter) {
    // big assumption!!!!
    // only considering a filter for basic columns
    // not taking into consideration the actual comparator
    FILTER_TYPE fType = filter.getSimpleFilterType();
    if (fType == FILTER_TYPE.COL_TO_VALUES) {
      return processColToValFilter(
          (IQuerySelector) filter.getLComparison().getValue(), filter.getRComparison().getValue());
    } else if (fType == FILTER_TYPE.VALUES_TO_COL) {
      return processColToValFilter(
          (IQuerySelector) filter.getRComparison().getValue(), filter.getLComparison().getValue());
    } else if (fType == FILTER_TYPE.VALUE_TO_VALUE) {
      return processValToValFilter(
          filter.getRComparison().getValue(), filter.getLComparison().getValue());
    } else {
      return null;
    }
  }

  private StringBuilder processValToValFilter(Object value1, Object value2) {
    // account for arrays
    // TODO: too lazy atm
    StringBuilder finalVal = new StringBuilder(value2 + "=");
    String value1String = value1 + "";

    if (value1 instanceof Vector) {
      Vector vecValue1 = (Vector) value1;
      StringBuffer rightValMaker = new StringBuffer("ARRAY");
      for (int valIndex = 0; valIndex < vecValue1.size(); valIndex++) {
        if (valIndex != 0) {
          rightValMaker.append("<>");
        }
        rightValMaker.append(vecValue1.get(valIndex));
      }
      rightValMaker.append("ARRAY");
      finalVal.append(rightValMaker.toString());
    } else finalVal.append(value1);

    return finalVal;
  }

  private StringBuilder processColToValFilter(IQuerySelector col, Object val) {
    String alias = col.getAlias();
    String valStr = null;
    if (val instanceof List) {
      StringBuffer rightValMaker = new StringBuffer("ARRAY");
      for (int valIndex = 0; valIndex < ((List) val).size(); valIndex++) {
        if (valIndex != 0) {
          rightValMaker.append("<>");
        }
        rightValMaker.append(((List) val).get(valIndex));
      }
      rightValMaker.append("ARRAY");
      valStr = rightValMaker.toString();
    } else {
      valStr = val.toString();
    }
    return new StringBuilder(alias + "=" + valStr);
  }
}
