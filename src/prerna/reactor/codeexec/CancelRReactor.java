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
package prerna.reactor.codeexec;

import org.apache.logging.log4j.Logger;
import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CancelRReactor extends AbstractReactor {

  private static final String CLASS_NAME = CancelRReactor.class.getName();

  @Override
  public NounMetadata execute() {
    Logger logger = getLogger(CLASS_NAME);
    AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
    rJavaTranslator.startR();
    boolean cancelled = rJavaTranslator.cancelExecution();
    return new NounMetadata(cancelled, PixelDataType.BOOLEAN);
  }
}
