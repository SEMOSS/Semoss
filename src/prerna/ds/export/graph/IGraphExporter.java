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
package prerna.ds.export.graph;

import java.awt.Color;
import java.util.Map;

public interface IGraphExporter {

  /**
   * Boolean if there are more edges to return
   *
   * @return
   */
  boolean hasNextEdge();

  Map<String, Object> getNextEdge();

  /**
   * Boolean if there are more vertices to return
   *
   * @return
   */
  boolean hasNextVert();

  Map<String, Object> getNextVert();

  /**
   * Return the count of each vert type
   *
   * @return
   */
  Map<String, Integer> getVertCounts();

  /**
   * Get a string representation of the node color
   *
   * @param c
   * @return
   */
  static String getRgb(Color c) {
    return c.getRed() + "," + c.getGreen() + "," + c.getBlue();
  }

  Object getData();
}
