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

import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RemoveDuplicateRowsReactor extends AbstractPyFrameReactor {

  /** This reactor removes duplicate rows from the data frame There are no inputs needed */
  @Override
  public NounMetadata execute() {
    // get frame
    PandasFrame frame = (PandasFrame) getFrame();

    // get table name
    String wrapperFrameName = frame.getWrapperName();

    // define the r script to be executed
    String script = wrapperFrameName + ".drop_dup()";

    // execute the r script
    frame.runScript(script);
    this.addExecutedCode(script);

    // NEW TRACKING
    UserTrackerFactory.getInstance()
        .trackAnalyticsWidget(
            this.insight,
            frame,
            "RemoveDuplicateRows",
            AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

    return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
  }
}
