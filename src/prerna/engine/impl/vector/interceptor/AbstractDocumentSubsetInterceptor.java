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
package prerna.engine.impl.vector.interceptor;

import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import prerna.engine.api.IVectorDatabaseEngine;

public abstract class AbstractDocumentSubsetInterceptor extends AbstractInterceptor {

  protected final Set<String> documents;

  public AbstractDocumentSubsetInterceptor(
      IVectorDatabaseEngine proxyEngine,
      IVectorDatabaseEngine targetEngine,
      Object[] constructorArgs) {
    super(proxyEngine, targetEngine, constructorArgs);
    // null documents means no filter needed.
    // empty documents filter means no files allowed.
    if (constructorArgs != null) {
      if (constructorArgs.length > 0) {
        String[] stringArgs = new String[constructorArgs.length];
        for (int i = 0; i < constructorArgs.length; i++) {
          stringArgs[i] = constructorArgs[i] == null ? null : constructorArgs[i].toString();
        }
        Set<String> documentsGiven = Sets.newHashSet(stringArgs);
        if (documentsGiven.contains("*")) {
          documents = null;
        } else {
          documents = documentsGiven;
        }
      } else {
        documents = new HashSet<>();
      }
    } else {
      documents = null;
    }
  }
}
