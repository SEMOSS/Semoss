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
package prerna.reactor.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RunLSILearnedReactor extends AbstractRFrameReactor {

  public RunLSILearnedReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.NUMERIC_VALUE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    // get pixel inputs
    organizeKeys();
    // get R connection
    init();
    String[] packages = new String[] {"LSAfun", "text2vec"};
    this.rJavaTranslator.checkPackages(packages);
    // output frame name
    RDataTable frame = (RDataTable) getFrame();
    String returnTable = frame.getName();

    String frameJoinCol = this.keyValue.get(this.keysToGet[0]);
    int numRows = Integer.parseInt(this.keyValue.get(this.keysToGet[1]));

    // path to your custom r script
    String rScriptPath2 = getBaseFolder() + "\\R\\UserScripts\\RunLSILearned2.r";
    String rScriptPath1 = getBaseFolder() + "\\R\\UserScripts\\lsi_lookup_learned.r";
    rScriptPath1 = rScriptPath1.replace("\\", "/");
    rScriptPath2 = rScriptPath2.replace("\\", "/");

    // embed r script in java
    StringBuilder rsb = new StringBuilder();
    // load r packages

    rsb.append("numMatch<-" + numRows + ";");
    rsb.append("source(\"" + rScriptPath1 + "\");");
    String readDescriptions =
        "Description<-data.frame(gsub(\"_\",\" \"," + returnTable + "[," + frameJoinCol + "]));";
    rsb.append(readDescriptions);
    String alterFrameSpace =
        returnTable
            + "$"
            + frameJoinCol
            + "<-gsub(\" \",\"_\","
            + returnTable
            + "[,"
            + frameJoinCol
            + "]);";
    rsb.append(alterFrameSpace);
    rsb.append("LSAspace <- readRDS(\"lsalearned.rds\");");
    rsb.append("source(\"" + rScriptPath2 + "\");");

    String leftTableName = returnTable;
    // TODO Change dfFinal to random string generated. Edit the R script.
    String rightTableName = "dfFinal";

    // TODO Change dfFinal to random string generated. Edit the R script.

    rsb.append("dfFinal$Description<-gsub(\" \",\"_\"," + "dfFinal$Description);");
    rsb.append(
        returnTable
            + "$LSA_Score<-NULL;"
            + returnTable
            + "$LSA_Category<-NULL;"
            + returnTable
            + "$Match<-NULL;");
    // only a single join type can be passed at a time
    String joinType = "left.outer.join";
    List<Map<String, String>> joinCols = new ArrayList<Map<String, String>>();

    Map<String, String> joinColMapping = new HashMap<String, String>();
    // TODO Change Description to something else
    joinColMapping.put(frameJoinCol, "Description");
    joinCols.add(joinColMapping);

    // execute r command
    String mergeString =
        RSyntaxHelper.getMergeSyntax(
            returnTable, leftTableName, rightTableName, joinType, joinCols);
    rsb.append(mergeString);
    rsb.append(";");

    // run script
    this.rJavaTranslator.runR(rsb.toString());

    return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
  }
}
