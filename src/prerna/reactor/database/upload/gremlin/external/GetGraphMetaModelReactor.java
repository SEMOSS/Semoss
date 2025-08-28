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
package prerna.reactor.database.upload.gremlin.external;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.masterdatabase.util.GenerateMetamodelLayout;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.GraphUtility;
import prerna.util.MyGraphIoMappingBuilder;
import prerna.util.UploadInputUtility;

public class GetGraphMetaModelReactor extends AbstractReactor {

  protected static final Logger classLogger = LogManager.getLogger(GetGraphMetaModelReactor.class);

  public GetGraphMetaModelReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.FILE_PATH.getKey(),
          ReactorKeysEnum.SPACE.getKey(),
          ReactorKeysEnum.GRAPH_TYPE_ID.getKey(),
          ReactorKeysEnum.USE_LABEL.getKey()
        };
  }

  @Override
  public NounMetadata execute() {
    /*
     * Get Inputs
     */
    organizeKeys();
    String fileName = UploadInputUtility.getFilePath(this.store, this.insight);
    if (fileName == null) {
      SemossPixelException exception =
          new SemossPixelException(
              new NounMetadata(
                  "Requires fileName to get graph metamodel.",
                  PixelDataType.CONST_STRING,
                  PixelOperationType.ERROR));
      exception.setContinueThreadOfExecution(false);
      throw exception;
    }
    boolean useLabel = useLabel();
    String graphTypeId = this.keyValue.get(this.keysToGet[1]);
    if (!useLabel) {
      if (graphTypeId == null) {
        SemossPixelException exception =
            new SemossPixelException(
                new NounMetadata(
                    "Requires graphTypeId to get graph metamodel.",
                    PixelDataType.CONST_STRING,
                    PixelOperationType.ERROR));
        exception.setContinueThreadOfExecution(false);
        throw exception;
      }
    }

    Map<String, Object> retMap = new HashMap<String, Object>();
    TinkerEngine.TINKER_DRIVER tinkerDriver = TinkerEngine.TINKER_DRIVER.NEO4J;
    if (new File(fileName).isFile() && fileName.contains(".")) {
      String fileExtension = fileName.substring(fileName.indexOf(".") + 1);
      tinkerDriver = TinkerEngine.TINKER_DRIVER.valueOf(fileExtension.toUpperCase());
    }
    Graph g = null;
    /*
     * Open Graph
     */
    if (tinkerDriver == TinkerEngine.TINKER_DRIVER.NEO4J) {
      File f = new File(fileName);
      if (f.exists() && f.isDirectory()) {
        g = Neo4jGraph.open(fileName);
      } else {
        SemossPixelException exception =
            new SemossPixelException(
                new NounMetadata(
                    "Invalid Neo4j path", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
        exception.setContinueThreadOfExecution(false);
        throw exception;
      }
    } else {
      g = TinkerGraph.open();
      try {
        File f = new File(fileName);
        if (!f.exists()) {
          SemossPixelException exception =
              new SemossPixelException(
                  new NounMetadata(
                      "Invalid graph path", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
          exception.setContinueThreadOfExecution(false);
          throw exception;
        }
        if (tinkerDriver == TinkerEngine.TINKER_DRIVER.TG) {
          // user kyro to de-serialize the cached graph
          Builder<GryoIo> builder = GryoIo.build();
          builder.graph(g);
          builder.onMapper(new MyGraphIoMappingBuilder());
          GryoIo reader = builder.create();
          reader.readGraph(fileName);
        } else if (tinkerDriver == TinkerEngine.TINKER_DRIVER.JSON) {
          // user kyro to de-serialize the cached graph
          Builder<GraphSONIo> builder = GraphSONIo.build();
          builder.graph(g);
          builder.onMapper(new MyGraphIoMappingBuilder());
          GraphSONIo reader = builder.create();
          reader.readGraph(fileName);
        } else if (tinkerDriver == TinkerEngine.TINKER_DRIVER.XML) {
          Builder<GraphMLIo> builder = GraphMLIo.build();
          builder.graph(g);
          builder.onMapper(new MyGraphIoMappingBuilder());
          GraphMLIo reader = builder.create();
          reader.readGraph(fileName);
        } else {
          throw new IllegalArgumentException("Can only process .tg, .json, and .xml files");
        }
      } catch (IOException e) {
        classLogger.error(Constants.STACKTRACE, e);
      }
    }

    if (g != null) {
      if (useLabel) {
        retMap = GraphUtility.getMetamodel(g.traversal());
      } else {
        retMap = GraphUtility.getMetamodel(g.traversal(), graphTypeId);
      }
      try {
        g.close();
      } catch (Exception e) {
        classLogger.error(Constants.STACKTRACE, e);
      }
    }

    // position tables in metamodel to be spaced and not overlap
    Map<String, Map<String, Double>> nodePositionMap =
        GenerateMetamodelLayout.generateMetamodelLayoutForGraphDBs(retMap);
    retMap.put(Constants.POSITION_PROP, nodePositionMap);

    return new NounMetadata(retMap, PixelDataType.MAP);
  }

  /**
   * Query the external db with a label to get the node
   *
   * @return
   */
  private boolean useLabel() {
    GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.USE_LABEL.getKey());
    if (grs != null && !grs.isEmpty()) {
      return (boolean) grs.get(0);
    }
    return false;
  }
}
