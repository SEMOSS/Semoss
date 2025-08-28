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
package prerna.engine.impl.vector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import prerna.query.interpreters.sql.SqlInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MilvusVectorQueryFitlerTranslationHelper {

  public StringBuilder processMilvusFilter(IQueryFilter filter) {
    IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
    if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
      return processSimpleQueryFilterForMilvus((SimpleQueryFilter) filter);
    } else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
      return processAndQueryFilterForMilvus((AndQueryFilter) filter);
    } else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
      return processOrQueryFilterForMilvus((OrQueryFilter) filter);
    }
    return null;
  }

  protected StringBuilder processOrQueryFilterForMilvus(OrQueryFilter filter) {
    StringBuilder filterBuilder = new StringBuilder();
    List<IQueryFilter> filterList = filter.getFilterList();
    int numAnds = filterList.size();
    for (int i = 0; i < numAnds; i++) {
      if (i > 0) {
        filterBuilder.append(" OR ");
      }
      filterBuilder.append(processMilvusFilter(filterList.get(i)));
    }

    return filterBuilder;
  }

  protected StringBuilder processAndQueryFilterForMilvus(AndQueryFilter filter) {
    StringBuilder filterBuilder = new StringBuilder();
    List<IQueryFilter> filterList = filter.getFilterList();
    int numAnds = filterList.size();

    for (int i = 0; i < numAnds; i++) {
      if (i > 0) {
        filterBuilder.append(" AND ");
      }
      filterBuilder.append(processMilvusFilter(filterList.get(i)));
    }

    return filterBuilder;
  }

  protected StringBuilder processSimpleQueryFilterForMilvus(SimpleQueryFilter filter) {
    NounMetadata leftComparison = filter.getLComparison();
    NounMetadata rightComparison = filter.getRComparison();
    String comparator = filter.getComparator();

    // Handling rightComparison if it is a SelectQueryStruct
    if (rightComparison.getValue() instanceof SelectQueryStruct) {
      SelectQueryStruct selectQueryStruct = (SelectQueryStruct) rightComparison.getValue();
      List<IQuerySelector> querySelectors = selectQueryStruct.getSelectors();

      if (!querySelectors.isEmpty() && querySelectors.get(0) instanceof QueryArithmeticSelector) {
        QueryArithmeticSelector arithmeticSelector =
            (QueryArithmeticSelector) querySelectors.get(0);

        IQuerySelector leftOperand = arithmeticSelector.getLeftSelector();
        String operator = arithmeticSelector.getMathExpr();
        IQuerySelector rightOperand = arithmeticSelector.getRightSelector();

        return new StringBuilder()
            .append(leftComparison.getValue())
            .append(" ")
            .append(comparator)
            .append(" ")
            .append(leftOperand.toString())
            .append(" ")
            .append(operator)
            .append(" ")
            .append(rightOperand.toString());
      }
    }

    // Handling leftComparison if it is a SelectQueryStruct
    if (leftComparison.getValue() instanceof SelectQueryStruct) {
      SelectQueryStruct selectQueryStruct = (SelectQueryStruct) leftComparison.getValue();
      List<IQuerySelector> querySelectors = selectQueryStruct.getSelectors();

      if (!querySelectors.isEmpty() && querySelectors.get(0) instanceof QueryArithmeticSelector) {
        QueryArithmeticSelector arithmeticSelector =
            (QueryArithmeticSelector) querySelectors.get(0);

        IQuerySelector leftOperand = arithmeticSelector.getLeftSelector();
        String operator = arithmeticSelector.getMathExpr();
        IQuerySelector rightOperand = arithmeticSelector.getRightSelector();

        return new StringBuilder()
            .append(leftOperand.toString())
            .append(" ")
            .append(operator)
            .append(" ")
            .append(rightOperand.toString())
            .append(" ")
            .append(comparator)
            .append(" ")
            .append(rightComparison.getValue());
      }
    }

    return addSelectorToValuesMilvusFilter(leftComparison, rightComparison, comparator);
  }

  protected StringBuilder addSelectorToValuesMilvusFilter(
      NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
    // get the left side
    IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
    String leftDataType = leftSelector.getDataType();

    List<Object> objects = new ArrayList<>();
    // ugh... this is gross
    if (rightComp.getValue() instanceof Collection) {
      objects.addAll((Collection) rightComp.getValue());
    } else {
      objects.add(rightComp.getValue());
    }

    StringBuilder filterBuilder = new StringBuilder();
    filterBuilder.append(leftSelector).append(" ");
    SqlInterpreter getInterpreter = new SqlInterpreter();
    String myFilterFormatted =
        getInterpreter.getFormatedObject(leftDataType, objects, thisComparator);
    String trimmedComparator = thisComparator.trim();

    switch (trimmedComparator.toUpperCase()) {
      // Comparison operators
      case "==":
      case "!=":
      case ">":
      case "<":
      case "<=":
      case ">=":
        filterBuilder.append(thisComparator).append(" ").append(myFilterFormatted);
        break;
      // Range Operators
      case "IN":
        filterBuilder.append("IN [").append(myFilterFormatted).append("]");
        break;
      case "?LIKE":
        filterBuilder.append("LIKE").append(myFilterFormatted).append(" ");
        break;
      case "?NLIKE":
        filterBuilder.append("NOT IN [").append(myFilterFormatted).append("]");
        break;
      default:
        filterBuilder.append(thisComparator).append(" ").append(myFilterFormatted);
    }

    return filterBuilder;
  }
}
