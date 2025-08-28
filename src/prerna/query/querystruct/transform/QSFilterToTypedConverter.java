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
package prerna.query.querystruct.transform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import prerna.algorithm.api.SemossDataType;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.query.querystruct.selectors.QueryTypedColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QSFilterToTypedConverter {

  public static List<IQueryFilter> convertFilters(
      List<IQueryFilter> origFilters, String tableName, Map<String, SemossDataType> typeMap) {
    if (origFilters != null && !origFilters.isEmpty()) {
      List<IQueryFilter> convertedFilters = new ArrayList<IQueryFilter>();
      for (int i = 0; i < origFilters.size(); i++) {
        convertedFilters.add(convertFilter(origFilters.get(i), tableName, typeMap));
      }
      return convertedFilters;
    }
    return origFilters;
  }

  /**
   * Convert a filter Look at left hand side and right hand side If either is a column, try to
   * convert
   *
   * @param queryFilter
   * @param meta
   * @return
   */
  public static IQueryFilter convertFilter(
      IQueryFilter queryFilter, String tableName, Map<String, SemossDataType> typeMap) {
    if (queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
      return convertSimpleQueryFilter((SimpleQueryFilter) queryFilter, tableName, typeMap);
    } else if (queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.AND) {
      return convertAndQueryFilter((AndQueryFilter) queryFilter, tableName, typeMap);
    } else if (queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
      return convertOrQueryFilter((OrQueryFilter) queryFilter, tableName, typeMap);
    } else if (queryFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.BETWEEN) {
      return convertBetweenQueryFilter((BetweenQueryFilter) queryFilter, tableName, typeMap);
    } else {
      return null;
    }
  }

  private static IQueryFilter convertOrQueryFilter(
      OrQueryFilter queryFilter, String tableName, Map<String, SemossDataType> typeMap) {
    OrQueryFilter newF = new OrQueryFilter();
    List<IQueryFilter> andFilterList = queryFilter.getFilterList();
    for (IQueryFilter f : andFilterList) {
      newF.addFilter(convertFilter(f, tableName, typeMap));
    }
    return newF;
  }

  private static IQueryFilter convertAndQueryFilter(
      AndQueryFilter queryFilter, String tableName, Map<String, SemossDataType> typeMap) {
    AndQueryFilter newF = new AndQueryFilter();
    List<IQueryFilter> andFilterList = queryFilter.getFilterList();
    for (IQueryFilter f : andFilterList) {
      newF.addFilter(convertFilter(f, tableName, typeMap));
    }
    return newF;
  }

  private static SimpleQueryFilter convertSimpleQueryFilter(
      SimpleQueryFilter queryFilter, String tableName, Map<String, SemossDataType> typeMap) {
    NounMetadata newL = null;
    NounMetadata origL = queryFilter.getLComparison();
    if (origL.getNounType() == PixelDataType.COLUMN) {
      // need to convert
      newL =
          new NounMetadata(
              convertSelector((IQuerySelector) origL.getValue(), tableName, typeMap),
              PixelDataType.COLUMN);
    }
    // Not going to handle a subquery against the pgvector at this point..
    //		else if(origL.getNounType() == PixelDataType.QUERY_STRUCT) {
    //			SelectQueryStruct newQs = getPhysicalQs((SelectQueryStruct) origL.getValue(), tableName);
    //			newL = new NounMetadata(newQs, PixelDataType.QUERY_STRUCT);
    //		}
    else {
      newL = origL;
    }

    NounMetadata newR = null;
    NounMetadata origR = queryFilter.getRComparison();
    if (origR.getNounType() == PixelDataType.COLUMN) {
      // need to convert
      newR =
          new NounMetadata(
              convertSelector((IQuerySelector) origR.getValue(), tableName, typeMap),
              PixelDataType.COLUMN);
    }
    // Not going to handle a subquery against the pgvector at this point..
    //		else if(origR.getNounType() == PixelDataType.QUERY_STRUCT) {
    //			SelectQueryStruct newQs = getPhysicalQs((SelectQueryStruct) origR.getValue(), tableName);
    //			newR = new NounMetadata(newQs, PixelDataType.QUERY_STRUCT);
    //		}
    else {
      newR = origR;
    }

    SimpleQueryFilter newF = new SimpleQueryFilter(newL, queryFilter.getComparator(), newR);
    return newF;
  }

  private static BetweenQueryFilter convertBetweenQueryFilter(
      BetweenQueryFilter queryFilter, String tableName, Map<String, SemossDataType> typeMap) {
    // need to convert column to the full name
    queryFilter.setColumn(convertSelector(queryFilter.getColumn(), tableName, typeMap));
    return queryFilter;
  }

  /**
   * Modify the selectors
   *
   * @param selector
   * @return
   */
  public static IQuerySelector convertSelector(
      IQuerySelector selector, String tableName, Map<String, SemossDataType> typeMap) {
    IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
    if (selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
      return convertConstantSelector((QueryConstantSelector) selector);
    } else if (selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
      return convertColumnSelector((QueryColumnSelector) selector, tableName, typeMap);
    } else if (selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
      return convertFunctionSelector((QueryFunctionSelector) selector, tableName, typeMap);
    } else if (selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
      return convertArithmeticSelector((QueryArithmeticSelector) selector, tableName, typeMap);
    } else if (selectorType == IQuerySelector.SELECTOR_TYPE.IF_ELSE) {
      return convertIfElseSelector((QueryIfSelector) selector, tableName, typeMap);
    }
    return null;
  }

  private static IQuerySelector convertIfElseSelector(
      QueryIfSelector selector, String tableName, Map<String, SemossDataType> typeMap) {
    // get the condition first
    IQueryFilter condition = selector.getCondition();
    selector.setCondition(convertFilter(condition, tableName, typeMap));

    // get the precedent
    IQuerySelector precedent = selector.getPrecedent();
    selector.setPrecedent(convertSelector(precedent, tableName, typeMap));

    IQuerySelector antecedent = selector.getAntecedent();
    if (antecedent != null) selector.setAntecedent(convertSelector(antecedent, tableName, typeMap));

    return selector;
  }

  private static IQuerySelector convertColumnSelector(
      QueryColumnSelector selector, String tableName, Map<String, SemossDataType> typeMap) {
    String inputTable = selector.getTable();
    String inputColumn = selector.getColumn();

    if (inputColumn == null || inputColumn.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
      // this means the input table is actually the column
      return new QueryTypedColumnSelector(tableName + "__" + inputTable, typeMap.get(inputTable));
    }
    return selector;
  }

  private static IQuerySelector convertArithmeticSelector(
      QueryArithmeticSelector selector, String tableName, Map<String, SemossDataType> typeMap) {
    QueryArithmeticSelector newS = new QueryArithmeticSelector();
    newS.setLeftSelector(convertSelector(selector.getLeftSelector(), tableName, typeMap));
    newS.setRightSelector(convertSelector(selector.getRightSelector(), tableName, typeMap));
    newS.setMathExpr(selector.getMathExpr());
    newS.setAlias(selector.getAlias());
    return newS;
  }

  private static IQuerySelector convertFunctionSelector(
      QueryFunctionSelector selector, String tableName, Map<String, SemossDataType> typeMap) {
    QueryFunctionSelector newS = new QueryFunctionSelector();
    for (IQuerySelector innerS : selector.getInnerSelector()) {
      newS.addInnerSelector(convertSelector(innerS, tableName, typeMap));
    }
    newS.setFunction(selector.getFunction());
    newS.setDistinct(selector.isDistinct());
    newS.setAlias(selector.getAlias());
    newS.setAdditionalFunctionParams(selector.getAdditionalFunctionParams());
    return newS;
  }

  private static IQuerySelector convertConstantSelector(QueryConstantSelector selector) {
    // do nothing
    return selector;
  }
}
