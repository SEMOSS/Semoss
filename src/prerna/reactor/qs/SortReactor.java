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
package prerna.reactor.qs;

import java.util.List;
import java.util.Vector;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;

public class SortReactor extends AbstractQueryStructReactor {

  private static final String COLUMNS_KEY = ReactorKeysEnum.COLUMNS.getKey();
  private static final String DIRECTION_KEY = ReactorKeysEnum.SORT.getKey();

  public SortReactor() {
    this.keysToGet = new String[] {COLUMNS_KEY, DIRECTION_KEY};
  }

  protected AbstractQueryStruct createQueryStruct() {
    List<String> colInputs = getOrderByColumns();
    List<String> sortDirs = getSortDirections();
    int colSize = colInputs.size();
    int sortDirSize = sortDirs.size();
    for (int selectIndex = 0; selectIndex < colSize; selectIndex++) {
      String newSelector = colInputs.get(selectIndex);
      if (newSelector.contains("__")) {
        String[] selectorSplit = newSelector.split("__");
        if (sortDirSize > selectIndex) {
          ((SelectQueryStruct) this.qs)
              .addOrderBy(selectorSplit[0], selectorSplit[1], sortDirs.get(selectIndex));
        } else {
          ((SelectQueryStruct) this.qs).addOrderBy(selectorSplit[0], selectorSplit[1], "ASC");
        }
      } else {
        if (sortDirSize > selectIndex) {
          ((SelectQueryStruct) this.qs)
              .addOrderBy(
                  newSelector, SelectQueryStruct.PRIM_KEY_PLACEHOLDER, sortDirs.get(selectIndex));
        } else {
          ((SelectQueryStruct) this.qs)
              .addOrderBy(newSelector, SelectQueryStruct.PRIM_KEY_PLACEHOLDER, "ASC");
        }
      }
    }
    return this.qs;
  }

  /**
   * Get the order by columns These could be in the store or passed in cur row
   *
   * @return
   */
  private List<String> getOrderByColumns() {
    // if it was passed based on the key
    List<String> colInputs = new Vector<String>();
    GenRowStruct colsGrs = this.store.getNoun(COLUMNS_KEY);
    if (colsGrs != null) {
      int size = colsGrs.size();
      if (size > 0) {
        for (int i = 0; i < size; i++) {
          colInputs.add(colsGrs.get(i).toString());
        }
        return colInputs;
      }
    }

    // if it was passed directly in
    int size = this.curRow.size();
    for (int i = 0; i < size; i++) {
      colInputs.add(this.curRow.get(i).toString());
    }
    return colInputs;
  }

  /**
   * Directions will always be put in its key
   *
   * @return
   */
  private List<String> getSortDirections() {
    // if it was passed based on the key
    List<String> sortDirections = new Vector<String>();
    GenRowStruct colsGrs = this.store.getNoun(DIRECTION_KEY);
    if (colsGrs != null) {
      int size = colsGrs.size();
      if (size > 0) {
        for (int i = 0; i < size; i++) {
          sortDirections.add(colsGrs.get(i).toString());
        }
      }
    }
    return sortDirections;
  }
}
