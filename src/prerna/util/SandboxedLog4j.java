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

import java.util.Map;
import java.util.Set;

/**
 * The sandbox rules for the log4j API, which project reactor code shares with
 * the platform so a reactor logs through the platform's loggers. The API and
 * the types its logging methods take are shared, the rest of log4j is the
 * project's own copy, and the calls that replace or shut down the platform's
 * logging are refused.
 */
final class SandboxedLog4j {

	/**
	 * The packages shared without their sub-packages, which hold log4j's
	 * internals.
	 */
	static final Set<String> SHARED_EXACT_PACKAGES = Set.of("org.apache.logging.log4j",
			"org.apache.logging.log4j.message", "org.apache.logging.log4j.spi");

	/**
	 * Single classes shared out of a package that is not: the types log4j's logging
	 * methods take.
	 */
	static final Set<String> SHARED_CLASSES = Set.of("org.apache.logging.log4j.util.Supplier",
			"org.apache.logging.log4j.util.MessageSupplier", "org.apache.logging.log4j.util.BiConsumer",
			"org.apache.logging.log4j.util.TriConsumer", "org.apache.logging.log4j.util.StringBuilderFormattable",
			"org.apache.logging.log4j.util.ReadOnlyStringMap", "org.apache.logging.log4j.util.StringMap");

	/**
	 * Single classes kept out of a shared package, so a project gets its own
	 * sandboxed copy: the message that dumps every thread.
	 */
	static final Set<String> UNSHARED_CLASSES = Set.of("org.apache.logging.log4j.message.ThreadDumpMessage");

	/**
	 * Calls project reactor code cannot make, by owner then method name.
	 */
	static final Map<String, Set<String>> BLOCKED_CALLS = Map.of("org/apache/logging/log4j/LogManager",
			Set.of("setFactory", "shutdown"));

	private SandboxedLog4j() {
	}
}
