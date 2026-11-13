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
package prerna.query.parsers;

import org.apache.jena.rdf.model.impl.Util;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

/**
 * Reads the values of RDF literals into Java values.
 */
public final class RdfLiteralParser {

	private RdfLiteralParser() {

	}

	/**
	 * Checks that text is an xsd double that Java's parser also reads. The xsd
	 * infinity forms INF and -INF are left out, since Java spells infinity as
	 * Infinity.
	 *
	 * @param text the text to check, or null
	 * @return true when the text parses as a double
	 */
	public static boolean readsAsDouble(String text) {
		return text != null && XMLDatatypeUtil.isValidDouble(text) && !text.trim().endsWith("INF");
	}

	/**
	 * Returns the Java value a Jena literal holds: a double for a number, a boolean
	 * for a boolean, and the literal's text otherwise. The text of a plain literal
	 * that reads as a double, or as true or false, is returned as that value.
	 *
	 * @param literal the literal to read
	 * @return the value of the literal
	 */
	public static Object getJenaValue(org.apache.jena.rdf.model.Literal literal) {
		Object value = literal.getValue();
		if (value instanceof Number number) {
			return number.doubleValue();
		}
		if (value instanceof Boolean bool) {
			return bool;
		}
		String text = literal.getString();
		if (Util.isSimpleString(literal) || Util.isLangString(literal)) {
			if (readsAsDouble(text)) {
				return Double.parseDouble(text);
			}
			if ("true".equals(text) || "false".equals(text)) {
				return Boolean.valueOf(text);
			}
		}
		return text;
	}
}
