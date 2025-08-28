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
package prerna.ds;

import java.util.Comparator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public final class TinkerAlgorithmUtility {

  protected static final Logger LOGGER =
      LogManager.getLogger(TinkerAlgorithmUtility.class.getName());

  private TinkerAlgorithmUtility() {}

  /**
   * Return the paths that are cycles in the tinkerframe given a max tinker size
   *
   * @param tf
   * @param maxCycleSize
   * @return
   */
  public static String runLoopIdentifer(TinkerFrame tf, int cycleSize) {
    GraphTraversal<Vertex, Path> traversal =
        tf.g
            .traversal()
            .V()
            .as("a")
            .repeat(__.out().simplePath())
            .times(cycleSize)
            .where(__.out().as("a"))
            .path()
            .dedup()
            .by(
                __.unfold()
                    .order()
                    .by(
                        TinkerFrame.TINKER_ID,
                        new Comparator<String>() {

                          @Override
                          public int compare(String o1, String o2) {
                            return o1.toLowerCase().compareTo(o2.toLowerCase());
                          }
                        })
                    .dedup()
                    .fold());

    StringBuilder cycles = new StringBuilder();
    while (traversal.hasNext()) {
      Path p = traversal.next();
      int size = p.size();
      Vertex v = p.get(0);
      cycles.append(v.value(TinkerFrame.TINKER_ID) + "");
      for (int stepIdx = 1; stepIdx < size; stepIdx++) {
        v = p.get(stepIdx);
        cycles.append(", " + v.value(TinkerFrame.TINKER_ID));
      }
      cycles.append("\n");
    }

    return cycles.toString();
  }

  /**
   * Returns the nodes that are not connected to the provided instance
   *
   * @param tf
   * @param type
   * @param instance
   * @return
   */
  public static String runDisconnectedNodesIdentifier(
      TinkerFrame tf, String type, String instance) {
    GraphTraversal<Vertex, Object> traversal =
        tf.g
            .traversal()
            .V()
            .or(
                __.not(
                    __.V()
                        .has(TinkerFrame.TINKER_ID, type + ":" + instance)
                        .repeat(__.out())
                        .until(__.outE().count().is(0))),
                __.has(TinkerFrame.TINKER_ID, P.neq(type + ":" + instance)).bothE().count().is(0))
            .values(TinkerFrame.TINKER_ID)
            .order();

    StringBuilder islandVertices = new StringBuilder();
    while (traversal.hasNext()) {
      islandVertices.append(traversal.next()).append("\n");
    }

    return islandVertices.toString();
  }
}
