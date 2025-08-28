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
package prerna.algorithm.learning.util;

import java.util.List;
import java.util.Map;

public final class InstanceSimilarity {

  private InstanceSimilarity() {}

  public static double getInstanceSimilarity(
      List<Object[]> instance1,
      List<Object[]> instance2,
      boolean[] isNumeric,
      String[] attributeNames,
      Map<String, DuplicationReconciliation> dups) {
    double categoricalSim = calculateInstanceCategoricalSim(instance1, instance2, isNumeric);
    double numericalSim =
        calculateNumericalSim(instance1, instance2, isNumeric, attributeNames, dups);

    return categoricalSim + numericalSim;
  }

  private static double calculateNumericalSim(
      List<Object[]> instance1,
      List<Object[]> instance2,
      boolean[] isNumeric,
      String[] attributeNames,
      Map<String, DuplicationReconciliation> dups) {
    double sim = 0;
    int numNumeric = 0;
    for (int i = 0; i < isNumeric.length; i++) {
      if (isNumeric[i]) {
        numNumeric++;
        DuplicationReconciliation dupSolver = dups.get(attributeNames[i]);

        Double instance1Val = 0.0;
        if (instance1.size() > 1) {
          for (int j = 0; j < instance1.size(); j++) {
            dupSolver.addValue(instance1.get(j)[i]);
          }
          instance1Val = dupSolver.getReconciliatedValue();
          dupSolver.clearValue();
        } else {
          instance1Val = ((Number) instance1.get(0)[i]).doubleValue();
        }

        Double instance2Val = 0.0;
        if (instance2.size() > 1) {
          for (int j = 0; j < instance2.size(); j++) {
            dupSolver.addValue(instance2.get(j)[i]);
          }
          instance2Val = dupSolver.getReconciliatedValue();
          dupSolver.clearValue();
        } else {
          instance2Val = ((Number) instance2.get(0)[i]).doubleValue();
        }

        sim += Math.pow(instance1Val - instance2Val, 2);
      }
    }

    if (numNumeric == 0) {
      return sim;
    }

    return (1 - Math.sqrt(sim)) * ((double) numNumeric / isNumeric.length);
  }

  private static double calculateInstanceCategoricalSim(
      List<Object[]> instance1, List<Object[]> instance2, boolean[] isNumeric) {
    double sim = 0;
    int numCategorical = 0;
    for (int i = 0; i < isNumeric.length; i++) {
      if (!isNumeric[i]) {
        numCategorical++;
        int matchCount = 0;
        int totalCount = 0;

        for (Object[] values1 : instance1) {
          for (Object[] values2 : instance2) {
            if (values1[i].equals(values2[i])) {
              matchCount++;
            }
            totalCount++;
          }
        }
        if (totalCount == 0) {
          throw new IllegalArgumentException("totalCount");
        }
        sim += (double) matchCount / totalCount;
      }
    }

    if (numCategorical == 0) {
      return sim;
    }

    return sim / numCategorical * ((double) numCategorical / isNumeric.length);
  }
}
