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
package prerna.reactor.insights;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetInsightConfigReactor extends AbstractReactor {

  public static final String INSIGHT_CONFIG = "$INSIGHT_CONFIG";

  @Override
  public NounMetadata execute() {
    NounMetadata noun = this.curRow.getNoun(0);
    // this is just an echo, where i send it back to the FE
    NounMetadata data =
        new NounMetadata(noun.getValue(), noun.getNounType(), PixelOperationType.INSIGHT_CONFIG);
    this.insight.getVarStore().put(INSIGHT_CONFIG, data);
    return data;
  }
}
