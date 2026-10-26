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

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

/**
 * Masks Eclipse RDF4J types when audit logging. Repository, Sail and the single
 * pass QueryResult iterations all reach the serializer through execQuery.
 *
 * Values and statements are exempt because their implementations hold only the
 * data. The one store reference that exists, ValueStoreRevision on the native
 * store's values, is transient and so is already skipped.
 */
public class LoggingRdf4jTypeAdapterFactory extends AbstractLoggingRdfTypeAdapterFactory {

	private static final String RDF4J_PACKAGE_PREFIX = "org.eclipse.rdf4j.";

	@Override
	protected String getPackagePrefix() {
		return RDF4J_PACKAGE_PREFIX;
	}

	@Override
	protected boolean isValueType(Class<?> rawType) {
		return Value.class.isAssignableFrom(rawType) || Statement.class.isAssignableFrom(rawType);
	}
}
