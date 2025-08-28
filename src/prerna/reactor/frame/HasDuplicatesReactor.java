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
package prerna.reactor.frame;

import java.util.List;
import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class HasDuplicatesReactor extends AbstractFrameReactor {

  public HasDuplicatesReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.COLUMNS.getKey()};
  }

  @Override
  public NounMetadata execute() {
    ITableDataFrame dataframe = (ITableDataFrame) getFrame();
    List<String> frameCols = this.curRow.getAllColumns();
    // if one column passed in is unique throughout the flat structure
    // of the frame, then we are done
    for (int i = 0; i < frameCols.size(); i++) {
      // if we find one column that is not unique
      // we know that the result of all the col
      Boolean isColUnique = dataframe.isUniqueColumn(frameCols.get(i));
      if (isColUnique) {
        // we have a column that is unique
        // return false, we dont have duplicates
        return new NounMetadata(false, PixelDataType.BOOLEAN);
      }
    }
    // no columns were unique
    // return true, we do have duplicates
    return new NounMetadata(true, PixelDataType.BOOLEAN);
  }
}
