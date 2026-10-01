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
package prerna.logging;

import java.io.IOException;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * Serializes every type in an RDF store's package as an identity hash so that
 * audit logging never reflects into the store's internals.
 *
 * Matching on package rather than on a list of interfaces covers the concrete
 * implementation classes that actually reach the serializer, which are rarely
 * the published interfaces, and keeps working as engines change.
 */
public abstract class AbstractLoggingRdfTypeAdapterFactory implements TypeAdapterFactory {

	/**
	 * @return the package prefix to mask, including the trailing dot
	 */
	protected abstract String getPackagePrefix();

	@Override
	public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
		Class<?> rawType = type.getRawType();
		if (!isRdfStoreType(rawType)) {
			// returning null lets Gson fall through to the next factory
			return null;
		}
		return new TypeAdapter<T>() {

			@Override
			public void write(JsonWriter out, T value) throws IOException {
				if (value == null) {
					out.nullValue();
				} else {
					out.value(System.identityHashCode(value));
				}
			}

			@Override
			public T read(JsonReader in) {
				throw new UnsupportedOperationException(
						"RDF store types are only serialized for audit logging and cannot be read back: "
								+ rawType.getName());
			}
		};
	}

	/**
	 * Value types hold the RDF data itself, so they are worth keeping in the audit
	 * log and are reflected over normally. A store whose value implementations
	 * reference the store they came from cannot exempt anything.
	 *
	 * @param rawType
	 * @return
	 */
	protected boolean isValueType(Class<?> rawType) {
		return false;
	}

	/**
	 * Throwables are excluded so that RDF exceptions keep their stack traces in the
	 * audit log through LoggingThrowableSerializer.
	 *
	 * @param rawType
	 * @return
	 */
	private boolean isRdfStoreType(Class<?> rawType) {
		if (Throwable.class.isAssignableFrom(rawType)) {
			return false;
		}
		if (!rawType.getName().startsWith(getPackagePrefix())) {
			return false;
		}
		return !isValueType(rawType);
	}
}
