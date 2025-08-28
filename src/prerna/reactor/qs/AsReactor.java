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
import prerna.om.HeadersException;
import prerna.query.querystruct.AbstractQueryStruct;

public class AsReactor extends AbstractQueryStructReactor {

  @Override
  protected AbstractQueryStruct createQueryStruct() {
    // add the inputs from the store as well as this operation
    // first is all the inputs
    // really has one job pick the parent..
    // replace the as Name
    // the as name could come in as an array too
    // for now I will go with the name
    List<String> aliasInput = curRow.getAllStrValues();

    if (this.parentReactor != null && aliasInput != null && !aliasInput.isEmpty()) {
      int size = aliasInput.size();
      String[] aliasArray = new String[size];
      HeadersException headerChecker = HeadersException.getInstance();

      // I need to make sure there are no __ since it causes issues
      for (int i = 0; i < size; i++) {
        String origHeader = aliasInput.get(i).replaceAll("_{2}", "_");
        aliasArray[i] = headerChecker.recursivelyFixHeaders(origHeader, aliasArray);
      }
      parentReactor.setAs(aliasArray);
    }

    return qs;
  }

  @Override
  public void mergeUp() {
    // merge this reactor into the parent reactor
    init();
    createQueryStruct();
  }
}
