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
package prerna.reactor.sheet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.date.SemossDate;
import prerna.om.InsightSheet;
import prerna.reactor.panel.AbstractInsightPanelReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.gson.InsightSheetAdapter;
import prerna.util.gson.NumberAdapter;
import prerna.util.gson.SemossDateAdapter;

public class SetSheetStateReactor extends AbstractInsightPanelReactor {

  private static final Logger classLogger = LogManager.getLogger(SetSheetStateReactor.class);

  /*
   * This class is complimentary to GetSheetStateReactor
   */

  private static final Gson GSON =
      new GsonBuilder()
          .disableHtmlEscaping()
          .registerTypeAdapter(Double.class, new NumberAdapter())
          .registerTypeAdapter(SemossDate.class, new SemossDateAdapter())
          .create();

  private static final String STATE = "state";

  public SetSheetStateReactor() {
    this.keysToGet = new String[] {STATE};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String serialized = getSerialization();
    if (serialized == null) {
      throw new NullPointerException("Serialization of the sheet state is null");
    }
    // we will just read the serialization of the insight sheet
    InsightSheetAdapter adapter = new InsightSheetAdapter();
    InsightSheet insightSheet = null;
    try {
      insightSheet = adapter.fromJson(serialized);
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new IllegalArgumentException(
          "Exeption occurred reading the panel state with error: " + e.getMessage());
    }

    this.insight.getInsightSheets().put(insightSheet.getSheetId(), insightSheet);
    NounMetadata noun =
        new NounMetadata(insightSheet, PixelDataType.SHEET, PixelOperationType.CACHED_SHEET);
    return noun;
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(STATE)) {
      return "The serialization for the insight panel";
    }
    return super.getDescriptionForKey(key);
  }

  private String getSerialization() {
    GenRowStruct grs = this.store.getNoun(STATE);
    if (grs != null && !grs.isEmpty()) {
      List<String> strInput = grs.getAllStrValues();
      if (strInput != null && !strInput.isEmpty()) {
        return strInput.get(0);
      }
      List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
      if (mapInput != null && !mapInput.isEmpty()) {
        return GSON.toJson(mapInput.get(0));
      }
    }

    if (!this.curRow.isEmpty()) {
      List<String> strInput = this.curRow.getAllStrValues();
      if (strInput != null && !strInput.isEmpty()) {
        return strInput.get(0);
      }
      List<Object> mapInput = this.curRow.getValuesOfType(PixelDataType.MAP);
      if (mapInput != null && !mapInput.isEmpty()) {
        return GSON.toJson(mapInput.get(0));
      }
    }

    return null;
  }
}
