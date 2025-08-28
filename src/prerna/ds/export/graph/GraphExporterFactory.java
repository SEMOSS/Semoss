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
import prerna.ds.TinkerFrame;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class GraphExporterFactory {

  private GraphExporterFactory() {}

  /**
   * Based on the frame, get the correct exporter
   *
   * @param frame
   * @return
   */
  public static IGraphExporter getExporter(IDataMaker frame) {
    IGraphExporter graphExporter = null;
    if (frame instanceof TinkerFrame) {
      graphExporter = new TinkerFrameGraphExporter((TinkerFrame) frame);
    }
    //		else if(frame instanceof H2Frame) {
    //			graphExporter = new RdbmsGraphExporter((H2Frame) frame);
    //		} else if(frame instanceof RDataTable) {
    //			graphExporter = new RGraphExporter((RDataTable) frame);
    //		}

    return graphExporter;
  }

  public static IGraphExporter getExporter(IDataMaker frame, Map<String, Color> colorMap) {
    IGraphExporter graphExporter = null;
    if (frame instanceof TinkerFrame) {
      graphExporter = new TinkerFrameGraphExporter((TinkerFrame) frame, colorMap);
    }
    return graphExporter;
  }
}
