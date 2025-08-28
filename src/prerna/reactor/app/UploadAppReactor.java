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
package prerna.reactor.app;

import prerna.reactor.project.UploadProjectAppReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UploadAppReactor extends UploadProjectAppReactor {

  @Override
  protected boolean deleteIfExisting() {
    return true;
  }

  @Override
  public NounMetadata execute() {
    return super.execute();
  }

  @Override
  public String getReactorDescription() {
    return "Import an app from an exported .smss-app file";
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
      return "This is a required value containing the relative file path of the single .smss-app file to be imported";
    } else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
      return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
    } else if (key.equals(ReactorKeysEnum.GLOBAL.getKey())) {
      return "This is a required value to determine if the app is public or private";
    }
    return super.getDescriptionForKey(key);
  }
}
