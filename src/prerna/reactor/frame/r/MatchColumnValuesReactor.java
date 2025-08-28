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
package prerna.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class MatchColumnValuesReactor extends AbstractRFrameReactor {

  public MatchColumnValuesReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.COLUMN.getKey()};
  }

  @Override
  public NounMetadata execute() {
    init();
    organizeKeys();
    String column = this.keyValue.get(this.keysToGet[0]);
    String baseFolder = Utility.getBaseFolder().replace("\\", "/");

    // check if packages are installed
    String[] packages = {"stringdist"};
    this.rJavaTranslator.checkPackages(packages);

    StringBuilder rsb = new StringBuilder();
    // source script
    String bestMatchScript =
        "source(\"" + baseFolder + "/R/Recommendations/advanced_federation_blend.r\") ; ";
    rsb.append(bestMatchScript);

    // get single column input
    RDataTable frame = (RDataTable) getFrame();
    String frameName = frame.getName();
    String matchesTable = Utility.getRandomString(8);
    String col1 = matchesTable + "col1";

    // run script
    rsb.append(col1 + "<- as.character(" + frameName + "$" + column + ");");
    rsb.append(matchesTable + " <- self_match(" + col1 + ");");
    rsb.append(
        matchesTable + "<-" + matchesTable + "[order(unique(" + matchesTable + ")$distance),] ;");
    //		rsb.append(RSyntaxHelper.asDataTable(matchesTable, matchesTable));

    // garbage collection
    rsb.append("rm(" + col1 + ")");
    this.rJavaTranslator.runR(rsb.toString());
    this.addExecutedCode(rsb.toString());

    RDataTable returnTable = createNewFrameFromVariable(matchesTable);
    NounMetadata retNoun = new NounMetadata(returnTable, PixelDataType.FRAME);

    // get count of exact matches
    String exactMatchCount =
        this.rJavaTranslator.getString(
            "as.character(nrow(" + matchesTable + "[" + matchesTable + "$distance == 0,]))");
    if (exactMatchCount != null) {
      int val = Integer.parseInt(exactMatchCount);
      retNoun.addAdditionalReturn(new NounMetadata(val, PixelDataType.CONST_INT));
    } else {
      throw new IllegalArgumentException("No matches found.");
    }

    // NEW TRACKING
    UserTrackerFactory.getInstance()
        .trackAnalyticsWidget(
            this.insight,
            frame,
            "PredictSimilarColumnValues",
            AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

    this.insight.getVarStore().put(matchesTable, retNoun);
    return retNoun;
  }
}
