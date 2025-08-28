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
package prerna.query.querystruct.delete;

import java.util.List;
import prerna.query.interpreters.sql.SqlInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class DeleteSqlInterpreter extends SqlInterpreter {

  private String table;

  public DeleteSqlInterpreter(SelectQueryStruct qs) {
    this.qs = qs;
    this.frame = qs.getFrame();
    this.engine = qs.getEngine();
  }

  //////////////////////////////////////////// Compose Query
  // //////////////////////////////////////////////

  public String composeQuery() {
    addTable();
    addFilters();

    StringBuilder query = new StringBuilder("DELETE FROM ");
    query.append(table);

    int numFilters = this.filterStatements.size();
    for (int i = 0; i < numFilters; i++) {
      if (i == 0) {
        query.append(" WHERE ");
      } else {
        query.append(" AND ");
      }
      query.append(this.filterStatements.get(i).toString());
    }

    return query.toString();
  }

  /** This is set directly in the QueryDelete reactor */
  public void addTable() {
    List<IQuerySelector> selectors = qs.getSelectors();
    QueryColumnSelector t = (QueryColumnSelector) selectors.get(0);
    this.table = t.getTable();
  }

  //	public static void main(String[] args) {
  //		SelectQueryStruct qs = new SelectQueryStruct();
  //		qs.addSelector("table", "column");
  //		QueryColumnSelector tab = new QueryColumnSelector("Nominated__Title_FK");
  //		QueryColumnSelector tab2 = new QueryColumnSelector("Nominated__Revenue");
  //		NounMetadata fil1 = new NounMetadata(tab, PixelDataType.COLUMN);
  //		NounMetadata fil2 = new NounMetadata("Chocolat", PixelDataType.CONST_STRING);
  //		NounMetadata fil3 = new NounMetadata(tab2, PixelDataType.COLUMN);
  //		NounMetadata fil4 = new NounMetadata(300000, PixelDataType.CONST_INT);
  //		SimpleQueryFilter filter1 = new SimpleQueryFilter(fil2, "=", fil1);
  ////		SimpleQueryFilter filter2 = new SimpleQueryFilter(fil4, "=", fil3);
  //		qs.addExplicitFilter(filter1);
  ////		qs.addExplicitFilter(filter2);
  //		DeleteSqlInterpreter interpreter = new DeleteSqlInterpreter(qs);
  //		String s = interpreter.composeQuery();
  //		System.out.println(s);
  //	}

}
