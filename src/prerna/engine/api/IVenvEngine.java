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
package prerna.engine.api;

import java.util.List;
import java.util.Map;

public interface IVenvEngine extends IEngine {

  String VENV_TYPE = "VENV_TYPE";

  VenvTypeEnum getVenvType();

  /*
   * List all packages and the respective versions
   */
  List<Map<String, String>> listPackages() throws Exception;

  /*
   * Pull the requirements file from a remote repo using git
   */
  void pullRequirementsFile();

  /*
   * Instead of pulling a requirements file let it be uploaded
   */
  void uploadRequirementsFile(String filePath);

  /*
   * The actual process implementation to create the virtual environment and install the relevant packages
   */
  void createVirtualEnv() throws Exception;

  /*
   * The requirements file has been updated and needs to be re-pulled
   */
  void updateVirtualEnv();

  /*
   *  Add a package to the venv. Restricted for Admins only
   */
  void addPackage(Map<String, Object> parameters) throws Exception;

  /*
   *  Remove a package to the venv. Restricted for Admins only
   */
  void removePackage(Map<String, Object> parameters) throws Exception;

  /*
   *  Get the path to the venv executable
   */
  String pathToExecutable();
}
