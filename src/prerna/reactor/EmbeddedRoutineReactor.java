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
package prerna.reactor;

import java.util.List;
import java.util.Vector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class EmbeddedRoutineReactor extends AbstractReactor {

  // this class does nothing!
  // it is meant when we have an embedded routine
  // within our main routine
  // but i just want to collect all the output in one location

  @Override
  public NounMetadata execute() {
    List<NounMetadata> nList = new Vector<NounMetadata>();
    for (String key : this.store.getNounKeys()) {
      if (key.equals(ALL_NOUN_STORE)) {
        continue;
      }
      GenRowStruct grs = this.store.getNoun(key);
      nList.add(grs.getNoun(0));
    }
    return new NounMetadata(nList, PixelDataType.VECTOR, PixelOperationType.VECTOR);
  }

  @Override
  public void mergeUp() {
    // merge this reactor into the parent reactor
    if (parentReactor != null) {
      for (String key : this.store.getNounKeys()) {
        if (key.equals(ALL_NOUN_STORE)) {
          continue;
        }
        GenRowStruct grs = this.store.getNoun(key);
        this.parentReactor.getCurRow().add(grs.getNoun(0));
      }
    }
  }
}
