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
package prerna.reactor.imports.union;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;

/** A factory class to dispense union routines based on the frame type. */
public abstract class UnionFactory {

  public static UnionRoutine getUnionRoutine(ITableDataFrame frame) {
    if (frame instanceof RDataTable) {
      return new RUnion();
    } else if (frame instanceof PandasFrame) {
      return new PyUnion();
    } else
      throw new IllegalArgumentException(
          "This frame type is not supported for union as of now. "
              + "Please convert frame to R or Python frame.");
  }
}
