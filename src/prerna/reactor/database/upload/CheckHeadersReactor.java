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
package prerna.reactor.database.upload;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import prerna.om.HeadersException;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CheckHeadersReactor extends AbstractReactor {

  public CheckHeadersReactor() {
    this.keysToGet = new String[] {"headerMap"};
  }

  @Override
  public NounMetadata execute() {
    Map<String, Object> headerMap = (Map) this.curRow.get(0);
    if (headerMap == null || headerMap.isEmpty()) {
      throw new IllegalArgumentException("Need to define " + this.keysToGet[0]);
    }

    HeadersException headerChecker = HeadersException.getInstance();
    Map<String, Map<String, String>> invalidHeadersMap =
        new Hashtable<String, Map<String, String>>();
    for (String sheetName : headerMap.keySet()) {
      List<String> userHeadersList = (List<String>) headerMap.get(sheetName);
      String[] userHeaders = userHeadersList.toArray(new String[userHeadersList.size()]);

      // now we need to check all of these headers
      for (int colIdx = 0; colIdx < userHeaders.length; colIdx++) {
        String userHeader = userHeaders[colIdx];
        Map<String, String> badHeaderMap = new Hashtable<String, String>();
        if (headerChecker.isIllegalHeader(userHeader)) {
          badHeaderMap.put(userHeader, "This header name is a reserved word");
        } else if (headerChecker.containsIllegalCharacter(userHeader)) {
          badHeaderMap.put(userHeader, "Header names cannot contain +%@;");
        } else if (headerChecker.isDuplicated(userHeader, userHeaders, colIdx)) {
          badHeaderMap.put(userHeader, "Cannot have duplicate header names");
        }

        // map is filled in only if the header is bad
        if (!badHeaderMap.isEmpty()) {
          Map<String, String> invalidHeadersForFile = null;
          if (invalidHeadersMap.containsKey(sheetName)) {
            invalidHeadersForFile = invalidHeadersMap.get(sheetName);
          } else {
            invalidHeadersForFile = new Hashtable<String, String>();
          }

          // now add in the bad header for the sheet map
          invalidHeadersForFile.putAll(badHeaderMap);
          // now store it in the overall object
          invalidHeadersMap.put(sheetName, invalidHeadersForFile);
        }
      }
    }
    if (invalidHeadersMap.isEmpty()) {
      return new NounMetadata(true, PixelDataType.BOOLEAN);
    } else {
      NounMetadata noun =
          new NounMetadata("Invalid Headers", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
      noun.addAdditionalReturn(new NounMetadata(invalidHeadersMap, PixelDataType.MAP));
      SemossPixelException exception = new SemossPixelException(noun);
      exception.setContinueThreadOfExecution(false);
      throw exception;
    }
  }
}
