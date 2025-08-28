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
package prerna.query.interpreters.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IDatabaseEngine;

public class SQLiteSqlInterpreter extends H2SqlInterpreter {

  public SQLiteSqlInterpreter() {}

  public SQLiteSqlInterpreter(IDatabaseEngine engine) {
    super(engine);
  }

  public SQLiteSqlInterpreter(ITableDataFrame frame) {
    super(frame);
  }

  @Override
  protected String formatDate(Object o, SemossDataType dateType) {
    if (o instanceof SemossDate) {
      return String.valueOf(((SemossDate) o).getZonedDateTime().toInstant().toEpochMilli());
    } else {
      SemossDate value = SemossDate.genDateObj(o + "");
      if (value != null) {
        return String.valueOf(value.getZonedDateTime().toInstant().toEpochMilli());
      }

      //			if(dateType == SemossDataType.DATE) {
      //				SemossDate value = SemossDate.genDateObj(o + "");
      //				if(value != null) {
      //					return String.valueOf(value.getDate().getTime());
      //				}
      //			} else {
      //				SemossDate value = SemossDate.genTimeStampDateObj(o + "");
      //				if(value != null) {
      //					return String.valueOf(value.getDate().getTime());
      //				}
      //			}
    }
    return o + "";
  }
}
