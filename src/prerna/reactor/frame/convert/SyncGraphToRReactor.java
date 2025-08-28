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
package prerna.reactor.frame.convert;

import org.apache.logging.log4j.Logger;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.engine.impl.tinker.iGraphUtilities;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SyncGraphToRReactor extends AbstractRFrameReactor {

  private static final String CLASS_NAME = SyncGraphToRReactor.class.getName();

  public SyncGraphToRReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.FRAME.getKey()};
  }

  @Override
  public NounMetadata execute() {
    init();
    organizeKeys();
    String[] packages = new String[] {"igraph"};
    this.rJavaTranslator.checkPackages(packages);
    this.rJavaTranslator.executeEmptyR("library(igraph)");

    ITableDataFrame frame = getFrame();
    if (!(frame instanceof TinkerFrame)) {
      throw new IllegalArgumentException("Frame must be a graph frame type");
    }
    Logger logger = getLogger(CLASS_NAME);

    TinkerFrame graph = (TinkerFrame) frame;
    AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(CLASS_NAME);
    String wd = this.insight.getInsightFolder();
    iGraphUtilities.synchronizeGraphToR(graph, rJavaTranslator, graph.getName(), wd, logger);
    return new NounMetadata(
        true, PixelDataType.BOOLEAN, PixelOperationType.FORCE_SAVE_DATA_TRANSFORMATION);
  }
}
