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
package prerna.engine.impl.tinker;

import java.util.List;
import org.apache.logging.log4j.Logger;
import prerna.ds.TinkerFrame;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.util.Constants;

public class iGraphUtilities {

  /**
   * @param graph
   * @param rJavaTranslator
   * @param graphName
   * @param wd
   * @param logger
   */
  public static void synchronizeGraphToR(
      TinkerFrame graph,
      AbstractRJavaTranslator rJavaTranslator,
      String graphName,
      String wd,
      Logger logger) {
    try {
      String filePath = TinkerUtilities.serializeGraph(graph, wd);
      filePath = filePath.replace("\\", "/");

      StringBuilder builder = new StringBuilder("library(\"igraph\");");
      builder.append(graphName + "<- read_graph(\"" + filePath + "\", \"graphml\");");
      // load the graph
      rJavaTranslator.executeEmptyR(builder.toString());
      graph.setIGraphSynched(true);
    } catch (Exception ex) {
      logger.error(Constants.STACKTRACE, ex);
      throw new IllegalArgumentException(
          "Could not convert TinkerFrame into iGraph. Please make sure iGraph package is installed.");
    }
  }

  /**
   * Delete nodes from R iGraph
   *
   * @param type
   * @param nodeList
   */
  public static void removeNodeFromR(
      String graphName,
      AbstractRJavaTranslator rJavaTranslator,
      String type,
      List<Object> nodeList,
      Logger logger) {
    for (int nodeIndex = 0; nodeIndex < nodeList.size(); nodeIndex++) {
      String name = type + ":" + nodeList.get(nodeIndex);
      try {
        logger.info("Deleting node = " + name);
        // eval is abstract and is determined by the specific R
        // implementation
        rJavaTranslator.executeEmptyR(
            graphName
                + " <- delete_vertices("
                + graphName
                + ", V("
                + graphName
                + ")[vertex_attr("
                + graphName
                + ", \""
                + TinkerFrame.TINKER_ID
                + "\") == \""
                + name
                + "\"])");
      } catch (Exception ex) {
        logger.error(Constants.STACKTRACE, ex);
        throw new IllegalArgumentException("Could not delete node = " + name);
      }
    }
  }
}
