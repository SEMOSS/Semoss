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
package prerna.reactor.frame.py;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class TimestampDataReactor extends AbstractPyFrameReactor {

  private static final String TIME_KEY = "time";

  public TimestampDataReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.NEW_COLUMN.getKey(), TIME_KEY};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    PandasFrame frame = (PandasFrame) getFrame();

    // get inputs
    String newColName = keyValue.get(this.keysToGet[0]);
    if (newColName == null || newColName.isEmpty()) {
      throw new IllegalArgumentException("Need to define the new column name");
    }
    String includeTime = this.keyValue.get(this.keysToGet[1]);
    Boolean includeT = Boolean.parseBoolean(includeTime);

    // check that the frame isn't null
    String wrapperFrameName = frame.getWrapperName();
    // check if new colName is valid
    newColName = getCleanNewColName(frame, newColName);
    if (newColName.contains("__")) {
      String[] split = newColName.split("__");
      wrapperFrameName = split[0];
      newColName = split[1];
    }

    String validNewHeader = getCleanNewColName(frame, newColName);
    if (validNewHeader.equals("")) {
      throw new IllegalArgumentException("Provide valid new column name (no special characters)");
    }

    OwlTemporalEngineMeta metadata = this.getFrame().getMetaData();
    metadata.addProperty(frame.getName(), frame.getName() + "__" + validNewHeader);
    metadata.setAliasToProperty(frame.getName() + "__" + validNewHeader, validNewHeader);

    String script = null;
    if (includeT) {
      script = wrapperFrameName + ".add_datetime_col('" + validNewHeader + "')";
      frame.runScript(script);
      this.addExecutedCode(script);
      metadata.setDataTypeToProperty(
          frame.getName() + "__" + newColName, SemossDataType.TIMESTAMP.toString());
    } else {
      script = wrapperFrameName + ".add_date_col('" + validNewHeader + "')";
      frame.runScript(script);
      this.addExecutedCode(script);
      metadata.setDataTypeToProperty(
          frame.getName() + "__" + newColName, SemossDataType.DATE.toString());
    }

    frame.syncHeaders();

    // NEW TRACKING
    UserTrackerFactory.getInstance()
        .trackAnalyticsWidget(
            this.insight,
            frame,
            "TimestampData",
            AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

    NounMetadata retNoun =
        new NounMetadata(
            frame,
            PixelDataType.FRAME,
            PixelOperationType.FRAME_HEADERS_CHANGE,
            PixelOperationType.FRAME_DATA_CHANGE);
    retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newColName));
    return retNoun;
  }
}
