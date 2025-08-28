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
package prerna.query.querystruct.evaluator;

import prerna.query.querystruct.selectors.QueryFunctionHelper;

public interface IQueryStructExpression {

  void processData(Object obj);

  Object getOutput();

  static IQueryStructExpression getExpression(String functionName) {
    functionName = functionName.toLowerCase();
    if (functionName.equalsIgnoreCase(QueryFunctionHelper.COUNT)) {
      return new QueryCountExpression();
    } else if (functionName.equalsIgnoreCase(QueryFunctionHelper.GROUP_CONCAT)) {
      return new QueryGroupConcatExpression();
    } else if (functionName.equalsIgnoreCase(QueryFunctionHelper.MAX)) {
      return new QueryMaxExpression();
    } else if (functionName.equalsIgnoreCase(QueryFunctionHelper.MEAN)
        || functionName.equalsIgnoreCase(QueryFunctionHelper.AVERAGE_1)
        || functionName.equalsIgnoreCase(QueryFunctionHelper.AVERAGE_2)) {
      return new QueryAverageExpression();
    } else if (functionName.equalsIgnoreCase(QueryFunctionHelper.MEDIAN)) {
      return new QueryMedianExpression();
    } else if (functionName.equalsIgnoreCase(QueryFunctionHelper.MIN)) {
      return new QueryMinExpression();
    } else if (functionName.equalsIgnoreCase(QueryFunctionHelper.STDEV_1)) {
      return new QueryStandardDeviationExpression();
    } else if (functionName.equalsIgnoreCase(QueryFunctionHelper.SUM)) {
      return new QuerySumExpression();
    } else if (functionName.equalsIgnoreCase(QueryFunctionHelper.UNIQUE_COUNT)) {
      return new QueryUniqueCountExpression();
    } else if (functionName.equalsIgnoreCase(QueryFunctionHelper.UNIQUE_GROUP_CONCAT)) {
      return new QueryUniqueGroupConcatExpression();
    }

    return null;
  }
}
