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
package prerna.reactor.project;

import prerna.auth.User;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.client.SocketClient;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class LoadAppReactor extends AbstractReactor {

  // takes in a the name and engine and mounts the engine assets as that variable
  // name in both python and R
  // I need to accomodate for when I should over ride
  // for instance a user could have saved a recipe with some mapping and then
  // later, they would like to use a different mapping

  public LoadAppReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey(), "loadPath"};
    this.keyRequired = new int[] {1, 0};
  }

  @Override
  public NounMetadata execute() {
    User user = insight.getUser();
    if (user == null) {
      NounMetadata noun =
          new NounMetadata(
              "User must be signed into an account in order to set app context",
              PixelDataType.CONST_STRING,
              PixelOperationType.ERROR,
              PixelOperationType.LOGGIN_REQUIRED_ERROR);
      SemossPixelException err = new SemossPixelException(noun);
      err.setContinueThreadOfExecution(false);
      throw err;
    }

    organizeKeys();
    String context = keyValue.get(keysToGet[0]);
    if (context == null || (context = context.trim()).isEmpty()) {
      return getError("Must pass in a valid project id for the context value");
    }
    boolean load = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[1]) + "");

    // need to replace the app with the
    boolean success = this.insight.setContext(context);
    // attempt once to directly map it with same name
    if (!success) {
      return getError("User does not have access to set the context to " + context);
    }

    // if we have a chroot, mount the project for that user.
    if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
      // get the app_root folder for the project
      String projectAppRootFolder = AssetUtility.getProjectAppRootFolder(context);
      this.insight.getUser().getUserSymlinkHelper().symlinkFolder(projectAppRootFolder);
    }

    // if python enabled
    // set the path
    if (PyUtils.pyEnabled()) {
      String assetsDir = AssetUtility.getProjectAssetsFolder(context).replace("\\", "/");
      String assetsPyDir = assetsDir + "/py";
      String script =
          "import sys\n"
              + "import os\n"
              + "sys.path.append('"
              + assetsDir
              + "')\n"
              + "sys.path.append('"
              + assetsPyDir
              + "')\n"
              + "os.chdir('"
              + assetsDir
              + "')";

      // if load, always grab the insight translator to set the path
      if (load) {
        PyTranslator pyTranslator = insight.getPyTranslator();
        pyTranslator.runEmptyPy(script);
      } else {
        // is the user already using python?
        // if so, set the path
        SocketClient sc = user.getPythonSocketClient(false);
        if (sc != null) {
          PyTranslator pyTranslator = insight.getPyTranslator();
          pyTranslator.runEmptyPy(script);
        }
      }
    }

    return new NounMetadata(
        "Successfully set app context to '" + context,
        PixelDataType.CONST_STRING,
        PixelOperationType.OPERATION);
  }

  @Override
  public String getReactorDescription() {
    return "Set the context for the insight in order to have access to app assets including custom reactors and python scripts";
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equalsIgnoreCase(this.keysToGet[1])) {
      return "Boolean if the path of the project should be loaded into the users process";
    }
    return super.getDescriptionForKey(key);
  }
}
