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

import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;

public class AsTaskReactor extends AbstractReactor {

  /**
   * This class is responsible for collecting the first element from a task and returning it as a
   * noun
   */
  public AsTaskReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.VALUE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    User user = this.insight.getUser();
    // throw error is user doesn't have rights to export data
    if (AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
      AbstractReactor.throwUserNotExporterError();
    }
    NounMetadata inputValue = getInputValue();
    List<Object[]> dataValues = new Vector<Object[]>();
    dataValues.add(new Object[] {inputValue.getValue()});

    ConstantDataTask task = new ConstantDataTask();
    Map<String, Object> returnData = new Hashtable<String, Object>();
    returnData.put("values", dataValues);
    returnData.put("headers", new String[] {"constant"});
    task.setOutputData(returnData);

    return new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
  }

  private NounMetadata getInputValue() {
    GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
    if (grs != null && !grs.isEmpty()) {
      return grs.getNoun(0);
    }

    Set<String> inKeys = new HashSet<String>(this.store.getNounKeys());
    inKeys.remove("all");
    inKeys.remove(this.keysToGet[0]);
    for (String k : inKeys) {
      grs = this.store.getNoun(k);
      if (grs != null && !grs.isEmpty()) {
        return grs.getNoun(0);
      }
    }

    if (this.curRow != null && !this.curRow.isEmpty()) {
      return this.curRow.getNoun(0);
    }

    return null;
  }
}
