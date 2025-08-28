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

import prerna.om.InsightSheet;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CachedSheetReactor extends AbstractReactor {

  /**
   * This code is the same as the Panel Reactor But it has a different op type
   *
   * <p>It is only intended to be used to simplify the cached insight recipe into a single call to
   * get the panel state instead of multiple calls for each portion of the insight
   */
  public CachedSheetReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.SHEET.getKey()};
  }

  @Override
  public NounMetadata execute() {
    // first input is the name of the sheet
    organizeKeys();
    String sheetId = this.keyValue.get(this.keysToGet[0]);
    InsightSheet insightSheet = this.insight.getInsightSheet(sheetId);
    if (insightSheet == null) {
      throw new NullPointerException("Sheet Id " + sheetId + " does not exist");
    }
    NounMetadata noun =
        new NounMetadata(insightSheet, PixelDataType.SHEET, PixelOperationType.CACHED_SHEET);
    return noun;
  }
}
