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
package prerna.reactor.frame.sql;

import java.util.HashMap;
import java.util.Map;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetParamValuesReactor extends AbstractReactor {

  public GetParamValuesReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.SQL.getKey(), ReactorKeysEnum.PARAM_KEY.getKey()};
    this.keyRequired = new int[] {1, 1};
  }

  @Override
  public NounMetadata execute() {
    // TODO Auto-generated method stub

    // replace the parameter
    // fill it
    // generate the sql
    // generate wrapper from it
    // parameterize and regenerate - need to know if front end needs it like that
    organizeKeys();

    try {
      String id = keyValue.get(keysToGet[0]); // this is the id
      String param = keyValue.get(keysToGet[1]);

      GenExpressionWrapper wrapper = this.insight.getSQLWrapper(id);
      String defQuery = wrapper.getQueryForParam(param);

      Map<String, Object> returnMap = new HashMap<String, Object>();
      returnMap.put("query", id);

      returnMap.put("params", wrapper.getAllParamNames());

      returnMap.put(param, defQuery);

      return new NounMetadata(returnMap, PixelDataType.MAP);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      return NounMetadata.getErrorNounMessage(e.getLocalizedMessage());
    }
  }
}
