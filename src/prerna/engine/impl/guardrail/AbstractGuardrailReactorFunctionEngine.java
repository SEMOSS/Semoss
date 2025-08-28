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
package prerna.engine.impl.guardrail;

import java.util.Properties;
import prerna.engine.api.IEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.engine.impl.function.AbstractReactorFunctionEngine;

public abstract class AbstractGuardrailReactorFunctionEngine extends AbstractReactorFunctionEngine
    implements IGuardrailReactorFunctionEngine {

  @Override
  public CATALOG_TYPE getCatalogType() {
    return IEngine.CATALOG_TYPE.GUARDRAIL;
  }

  @Override
  public String getCatalogSubType(Properties smssProp) {
    return "GUARDRAIL";
  }
}
