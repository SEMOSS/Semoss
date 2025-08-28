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
package prerna.reactor.export;

import prerna.ds.py.PyUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CollectPivotReactor extends AbstractReactor {

  public CollectPivotReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.ROW_GROUPS.getKey(),
          ReactorKeysEnum.COLUMNS.getKey(),
          ReactorKeysEnum.VALUES.getKey(),
          ReactorKeysEnum.FRAME_TYPE.getKey()
        };
  }

  public NounMetadata execute() {
    // default this to use Python
    // if Python not present
    // try in R
    // default is R
    String frameType = "R";
    if (store.getNoun(keysToGet[3]) != null) {
      frameType = keyValue.get(keysToGet[3]);
    }

    TaskBuilderReactor reactor = null;
    // frameType.equalsIgnoreCase("Py") &&
    if (PyUtils.pyEnabled()) {
      reactor = new prerna.reactor.frame.py.CollectPivotReactor();
    } else {
      reactor = new prerna.reactor.frame.r.CollectPivotReactor();
    }

    // pass the references/values
    // return the execution result
    reactor.In();
    reactor.setInsight(this.insight);
    reactor.setNounStore(this.store);
    return reactor.execute();
  }
}
