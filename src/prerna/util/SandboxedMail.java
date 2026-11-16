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
import java.util.Set;

/**
 * The sandbox rules for the Jakarta Mail and Activation APIs, which project
 * reactor code shares with the platform to build messages and attachments. The
 * classes that send and read mail cannot be used, since a project sends mail
 * through EmailUtility, and a path the APIs take as text follows the path rule
 * of {@link SandboxedJavaExecution}.
 */
final class SandboxedMail {

	/**
	 * The packages project reactor code shares with the platform, as name
	 * prefixes.
	 */
	static final List<String> SHARED_PACKAGES = List.of("jakarta.mail.", "jakarta.activation.");

	/**
	 * Classes project reactor code can never use, by full name; their nested
	 * classes are blocked with them. These send and read mail.
	 */
	static final Set<String> BLOCKED_CLASSES = Set.of("jakarta.mail.Transport", "jakarta.mail.Store",
			"jakarta.mail.Service", "jakarta.mail.Folder");

	/**
	 * Calls that take a path as text, by owner, then method name, then the position
	 * of the path argument.
	 */
	static final Map<String, Map<String, Integer>> PATH_TEXT_ARGUMENTS = Map.of(
			"jakarta/activation/FileDataSource", Map.of("<init>", 0),
			"jakarta/activation/MimetypesFileTypeMap", Map.of("<init>", 0),
			"jakarta/activation/MailcapCommandMap", Map.of("<init>", 0),
			"jakarta/mail/internet/MimeBodyPart", Map.of("attachFile", 0, "saveFile", 0));

	private SandboxedMail() {
	}
}
