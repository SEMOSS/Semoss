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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;


import prerna.engine.api.IVectorDatabaseEngine;

public abstract class AbstractDocumentSubsetInterceptor extends AbstractInterceptor {
	
	protected final Set<String> documents;
	
	public AbstractDocumentSubsetInterceptor(IVectorDatabaseEngine proxyEngine, IVectorDatabaseEngine targetEngine, Object[] constructorArgs) {
		super(proxyEngine, targetEngine, constructorArgs);
		// null documents means no filter needed.
		// empty documents filter means no files allowed.
		if(constructorArgs != null) {
			if(constructorArgs.length > 0){
				String[] stringArgs = new String[constructorArgs.length];
				for(int i=0; i<constructorArgs.length; i++) {
					stringArgs[i] = constructorArgs[i] == null ? null : constructorArgs[i].toString();
				}
				Set<String> documentsGiven = new TreeSet<>(UTF8_BYTE_ORDER_COMPARATOR);
				Collections.addAll(documentsGiven, stringArgs);
				if(documentsGiven.contains("*")) {
					documents = null;
				} else {
					documents = documentsGiven;
				}
			} else {
				documents = new TreeSet<>(UTF8_BYTE_ORDER_COMPARATOR);
			}
		} else {
			documents = null;
		}
	}
	
	private static final Comparator<String> UTF8_BYTE_ORDER_COMPARATOR = (s1, s2) -> {
	    if (s1 == null && s2 == null) return 0;
	    if (s1 == null) return -1;
	    if (s2 == null) return 1;
	    byte[] b1 = s1.getBytes(StandardCharsets.UTF_8);
	    byte[] b2 = s2.getBytes(StandardCharsets.UTF_8);
	    int len = Math.min(b1.length, b2.length);
	    for (int i = 0; i < len; i++) {
	        int cmp = Byte.compare(b1[i], b2[i]);
	        if (cmp != 0) return cmp;
	    }
	    return Integer.compare(b1.length, b2.length);
	};
	
}
