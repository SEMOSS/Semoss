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
package prerna.reactor.qs.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.om.RemoteItem;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class DropBoxListFilesReactor extends AbstractReactor {

  public DropBoxListFilesReactor() {
    this.keysToGet = new String[] {};
  }

  @Override
  public NounMetadata execute() {

    List<HashMap<String, Object>> masterList = new ArrayList<HashMap<String, Object>>();

    // lists the various files for this user

    String objectName = "prerna.om.RemoteItem"; // it will fill this object and return the data
    String[] beanProps = {"name", "path"}; // add is done when you have a list
    String jsonPattern = "matches[].{name:metadata.name, path:metadata.path_lower}";

    // api string
    String url_str = "https://api.dropboxapi.com/2/files/search";

    // get access token
    String accessToken = null;
    User user = this.insight.getUser();
    try {
      if (user == null) {
        Map<String, Object> retMap = new HashMap<String, Object>();
        retMap.put("type", "dropbox");
        retMap.put("message", "Please login to your DropBox account");
        throwLoginError(retMap);
      } else if (user != null) {
        AccessToken dropToken = user.getAccessToken(AuthProvider.DROPBOX);
        accessToken = dropToken.getAccess_token();
      }
    } catch (Exception e) {
      Map<String, Object> retMap = new HashMap<String, Object>();
      retMap.put("type", "dropbox");
      retMap.put("message", "Please login to your DropBox account");
      throwLoginError(retMap);
    }

    // you fill what you want to send on the API call
    Hashtable params = new Hashtable();
    params.put("path", "");
    params.put("query", ".csv");
    params.put("start", 0);
    params.put("max_results", 1000);
    params.put("mode", "filename");

    String output = HttpHelperUtility.makePostCall(url_str, accessToken, params, true);

    // fill the bean with the return
    Object C = BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
    System.out.println(C.getClass().getName());
    if (C instanceof RemoteItem) {
      RemoteItem fileList = (RemoteItem) C;
      HashMap<String, Object> tempMap = new HashMap<String, Object>();
      tempMap.put("name", fileList.getName());
      tempMap.put("path", fileList.getPath());
      masterList.add(tempMap);
    } else {
      List<RemoteItem> fileList =
          (List) BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
      for (RemoteItem entry : fileList) {
        HashMap<String, Object> tempMap = new HashMap<String, Object>();
        tempMap.put("name", entry.getName());
        tempMap.put("path", entry.getPath());
        masterList.add(tempMap);
      }
    }

    return new NounMetadata(
        masterList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CLOUD_FILE_LIST);
  }
}
