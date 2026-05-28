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
package prerna.reactor.qs;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import prerna.sablecc2.om.ReactorKeysEnum;

/**
 * Executes a Base64-encoded SPARQL SELECT query against an RDF database.
 */
public class SparqlQueryBase64Reactor extends AbstractSparqlQueryReactor {

	@Override
	protected String getDecodedQuery() {
		try {
			return new String(Base64.getDecoder().decode(this.keyValue.get(ReactorKeysEnum.QUERY_KEY.getKey())),
					StandardCharsets.UTF_8);
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to decode query: input is not a base64-encoded utf-8 string", e);
		}
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.QUERY_KEY.getKey())) {
			return "The SPARQL SELECT query to execute. The query should be passed as a base64-encoded utf-8 string";
		}
		return super.getDescriptionForKey(key);
	}

}
