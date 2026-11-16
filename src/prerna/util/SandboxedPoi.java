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
package prerna.util;

import java.util.List;
import java.util.Map;

/**
 * The sandbox rules for Apache POI, which project reactor code shares with the
 * platform for Excel and Office files. The POI family is shared with its schema
 * classes, and a path POI takes as text follows the path rule of
 * {@link SandboxedJavaExecution}.
 */
final class SandboxedPoi {

	/**
	 * The packages project reactor code shares with the platform, as name
	 * prefixes: POI, the streaming Excel reader, and the schema classes they read
	 * documents with.
	 */
	static final List<String> SHARED_PACKAGES = List.of("org.apache.poi.", "com.github.pjfanning.xlsx.",
			"org.openxmlformats.schemas.", "com.microsoft.schemas.", "org.etsi.uri.", "org.w3.x2000.");

	/**
	 * Calls that take a path as text, by owner, then method name, then the position
	 * of the path argument.
	 */
	static final Map<String, Map<String, Integer>> PATH_TEXT_ARGUMENTS = Map.of(
			"org/apache/poi/xssf/usermodel/XSSFWorkbook", Map.of("<init>", 0),
			"org/apache/poi/hslf/usermodel/HSLFSlideShow", Map.of("<init>", 0),
			"org/apache/poi/openxml4j/opc/OPCPackage", Map.of("open", 0, "create", 0),
			"org/apache/poi/ooxml/POIXMLDocument", Map.of("openPackage", 0));

	private SandboxedPoi() {
	}
}
