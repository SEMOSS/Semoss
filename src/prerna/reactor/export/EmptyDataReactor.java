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
package prerna.reactor.export;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskUtility;

public class EmptyDataReactor extends AbstractReactor {

  public EmptyDataReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.VALUE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    Object value = getValue();
    boolean noData = TaskUtility.noData(value);
    return new NounMetadata(noData, PixelDataType.BOOLEAN);
  }

  private Object getValue() {
    GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
    if (grs != null && !grs.isEmpty()) {
      return grs.get(0);
    }

    grs = this.store.getNoun(PixelDataType.FORMATTED_DATA_SET.toString());
    if (grs != null && !grs.isEmpty()) {
      return grs.get(0);
    }

    if (this.curRow != null && !this.curRow.isEmpty()) {
      return this.curRow.get(0);
    }

    throw new IllegalArgumentException("No data passed in");
  }
}
