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
package prerna.algorithm.learning.util;

public class MeanDistance implements IClusterDistanceMode {

  private double centroidValue;
  private double numInstances;
  private double emptyInstances;

  private double previousCentroidValue;
  private double changeToCentroidValue;
  private boolean previousNull;

  public MeanDistance() {}

  @Override
  public double getCentroidValue() {
    return this.centroidValue;
  }

  @Override
  public void addPartialToCentroidValue(Double newValue, double factor) {
    if (newValue == null) {
      previousNull = true;
      emptyInstances += factor;
      return;
    }

    previousNull = false;
    previousCentroidValue = centroidValue;
    changeToCentroidValue = (newValue * factor - previousCentroidValue) / (numInstances + 1);
    centroidValue += changeToCentroidValue;
    // numInstances++; do not increase numInstances for partial additions
  }

  @Override
  public void addToCentroidValue(Double newValue) {
    if (newValue == null) {
      previousNull = true;
      emptyInstances++;
      return;
    }

    previousNull = false;
    previousCentroidValue = centroidValue;
    changeToCentroidValue = (newValue - previousCentroidValue) / (numInstances + 1);
    centroidValue += changeToCentroidValue;
    numInstances++;
  }

  @Override
  public void removePartialFromCentroidValue(Double newValue, double factor) {
    if (newValue == null) {
      previousNull = true;
      emptyInstances -= factor;
      return;
    }

    previousNull = false;
    previousCentroidValue = centroidValue;
    if (numInstances == 1) {
      changeToCentroidValue = -1 * centroidValue;
      centroidValue = 0;
    } else {
      changeToCentroidValue = (-1 * newValue * factor + previousCentroidValue) / (numInstances - 1);
      centroidValue += changeToCentroidValue;
    }
    // numInstances--; do not decrease numInstances for partial additions
  }

  @Override
  public void removeFromCentroidValue(Double newValue) {
    if (newValue == null) {
      previousNull = true;
      emptyInstances--;
      return;
    }

    previousNull = false;
    previousCentroidValue = centroidValue;
    if (numInstances == 1) {
      changeToCentroidValue = -1 * centroidValue;
      centroidValue = 0;
    } else {
      changeToCentroidValue = (-1 * newValue + previousCentroidValue) / (numInstances - 1);
      centroidValue += changeToCentroidValue;
    }
    numInstances--;
  }

  @Override
  public double getNullRatio() {
    double e = (double) emptyInstances;
    double i = (double) numInstances;
    double total = e + i;
    if (total == 0) {
      return 0;
    } else {
      return e / (e + i);
    }
  }

  @Override
  public void reset() {
    centroidValue = 0;
    numInstances = 0;
    emptyInstances = 0;
    previousCentroidValue = 0;
    changeToCentroidValue = 0;
  }

  @Override
  public double getPreviousCentroidValue() {
    return this.previousCentroidValue;
  }

  @Override
  public Double getChangeToCentroidValue() {
    if (previousNull) {
      return null;
    }
    return this.changeToCentroidValue;
  }

  @Override
  public double getNumNull() {
    return this.emptyInstances;
  }

  @Override
  public double getNumInstances() {
    return this.numInstances;
  }

  @Override
  public boolean isPreviousNull() {
    return this.previousNull;
  }
}
