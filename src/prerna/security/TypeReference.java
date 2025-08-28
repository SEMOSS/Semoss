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
package prerna.security;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class TypeReference<T> {
	private final Type type;

	protected TypeReference() {
		Type superclass = getClass().getGenericSuperclass();
		if (superclass instanceof ParameterizedType) {
			this.type = ((ParameterizedType) superclass).getActualTypeArguments()[0];
		} else {
			throw new IllegalArgumentException("TypeReference must be created with a generic type.");
		}
	}

	public Type getType() {
		return this.type;
	}
}
