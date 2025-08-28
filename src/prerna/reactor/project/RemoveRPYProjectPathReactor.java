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
package prerna.reactor.project;

import prerna.ds.py.PyTranslator;
import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

public class RemoveRPYProjectPathReactor extends AbstractReactor {

  public RemoveRPYProjectPathReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    PyTranslator pyt = this.insight.getPyTranslator();
    AbstractRJavaTranslator rt = this.insight.getRJavaTranslator(this.getClass().getName());

    String projectId = keyValue.get(keysToGet[0]);
    String basePath = AssetUtility.getProjectAssetsFolder(projectId);
    String folderName = basePath + "/py";
    folderName = folderName.replace("\\", "/");

    if (pyt != null) {
      pyt.runScript("import sys", "sys.path.remove('" + folderName + "')");
    }
    if (rt != null) {
      rt.runR("setwd('" + "NA" + "')");
    }

    return NounMetadata.getSuccessNounMessage("Removed " + projectId + " to path");
  }

  @Override
  public String getReactorDescription() {
    return "Remove the project assets folder from the python sys.path and/or the R setwd";
  }
}
