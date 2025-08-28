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
import org.apache.logging.log4j.Logger;
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

public class SharePointDriveSelectorReactor extends AbstractReactor {

  private static final String CLASS_NAME = SharePointDriveSelectorReactor.class.getName();

  public SharePointDriveSelectorReactor() {
    this.keysToGet = new String[] {"siteId"};
  }

  @Override
  public NounMetadata execute() {

    Logger logger = getLogger(CLASS_NAME);
    organizeKeys();
    String siteID = this.keyValue.get(this.keysToGet[0]);
    if (siteID == null || siteID.length() <= 0) {
      throw new IllegalArgumentException("Need to specify the SharePoint server");
    }

    List<HashMap<String, Object>> masterList = new ArrayList<HashMap<String, Object>>();
    // lists the various files for this user

    // name of the object to return
    String objectName = "prerna.om.RemoteItem"; // it will fill this object and return the data
    String[] beanProps = {"name", "type", "id"}; // add is done when you have a list
    String jsonPattern = "value[].{name:name,driveType:driveType,id:id}";

    // get access token
    String accessToken = null;
    User user = this.insight.getUser();

    try {
      if (user == null) {
        Map<String, Object> retMap = new HashMap<String, Object>();
        retMap.put("type", "microsoft");
        retMap.put("message", "Please login to your Microsoft account");
        throwLoginError(retMap);
      } else if (user != null) {
        AccessToken msToken = user.getAccessToken(AuthProvider.MICROSOFT);
        accessToken = msToken.getAccess_token();
      }
    } catch (Exception e) {
      Map<String, Object> retMap = new HashMap<String, Object>();
      retMap.put("type", "microsoft");
      retMap.put("message", "Please login to your Microsoft account");
      throwLoginError(retMap);
    }

    // add in params for the get call
    Hashtable params = new Hashtable();
    // params.put("select", "name,id");
    String url_str = "https://graph.microsoft.com/v1.0/sites/" + siteID + "/drives";
    String output = HttpHelperUtility.makeGetCall(url_str, accessToken, params, true);

    // fill the bean with the return
    // fill an object
    Object C = BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
    // check if the object if a remote item or a vector
    // if its a remote item add it to the master list
    // System.out.println(C.getClass().getName());
    if (C instanceof RemoteItem) {
      RemoteItem fileList = (RemoteItem) C;
      HashMap<String, Object> tempMap = new HashMap<String, Object>();
      tempMap.put("type", fileList.getType());
      tempMap.put("name", fileList.getName());
      tempMap.put("id", fileList.getId());
      masterList.add(tempMap);
    }
    // if its a list, iterate through it and add it to the master list
    else {
      List<RemoteItem> fileList =
          (List) BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
      for (RemoteItem entry : fileList) {
        HashMap<String, Object> tempMap = new HashMap<String, Object>();
        tempMap.put("type", entry.getType());
        tempMap.put("name", entry.getName());
        tempMap.put("id", entry.getId());
        masterList.add(tempMap);
      }
    }

    return new NounMetadata(
        masterList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CLOUD_FILE_LIST);
  }
}
