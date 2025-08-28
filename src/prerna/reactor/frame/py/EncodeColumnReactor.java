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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class EncodeColumnReactor extends AbstractPyFrameReactor {

  @Override
  public NounMetadata execute() {
    organizeKeys();
    PandasFrame frame = (PandasFrame) getFrame();
    String frameName = frame.getName();
    List<String> scripts = new ArrayList<>();

    scripts.add("import hashlib");
    scripts.add("def encode(value) : return hashlib.sha256(str(value).encode()).hexdigest()");
    List<String> columns =
        this.store.nounRow.get("columns").getVector().stream()
            .map(noun -> noun.getValue().toString())
            .collect(Collectors.toList());
    for (String col : columns) {
      scripts.add(frameName + "['" + col + "'] = " + frameName + "['" + col + "'].apply(encode)");
    }
    String[] scriptArray = new String[scripts.size()];
    scripts.toArray(scriptArray);
    insight.getPyTranslator().runEmptyPy(scriptArray);
    for (String script : scripts) {
      this.addExecutedCode(script);
    }

    // upon successful execution
    OwlTemporalEngineMeta metadata = frame.getMetaData();
    for (String col : columns) {
      // set the type for all the columns to be string
      metadata.modifyDataTypeToProperty(
          frameName + "__" + col, frameName, SemossDataType.STRING.toString());
    }

    // NEW TRACKING
    UserTrackerFactory.getInstance()
        .trackAnalyticsWidget(
            this.insight,
            frame,
            "EncodeColumnReactor",
            AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

    return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
  }
}
