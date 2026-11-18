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
package prerna.util.sandbox;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The environment a jailed child process gets: the server's own is not passed on. Tomcat's
 * environment can hold database passwords, cloud keys and {@code LOCAL_KEY}, and none of that
 * should be readable by user code.
 */
public final class SandboxEnv {

	private static final Logger classLogger = LogManager.getLogger(SandboxEnv.class);

	/** Never secret, and tools misbehave without them. */
	private static final Set<String> ALWAYS_KEPT = Set.of("PATH", "LANG", "LANGUAGE", "LC_ALL", "TZ");

	private SandboxEnv() {
		// static utility
	}

	/** The names kept: the fixed set plus {@code SANDBOX_ENV_ALLOWLIST}. */
	public static Set<String> kept() {
		Set<String> kept = new HashSet<>(ALWAYS_KEPT);
		String extra = SandboxSettings.SANDBOX_ENV_ALLOWLIST.configured();
		if (extra != null) {
			for (String name : extra.split(",")) {
				if (!name.trim().isEmpty()) {
					kept.add(name.trim());
				}
			}
		}
		return kept;
	}

	/**
	 * Removes every variable that is not kept from a child's environment, in place.
	 *
	 * @param env the child's environment, from {@code ProcessBuilder.environment()}
	 */
	public static void scrub(Map<String, String> env) {
		Set<String> kept = kept();
		int before = env.size();
		env.keySet().removeIf(name -> !kept.contains(name));
		classLogger.debug("Scrubbed {} of {} environment variables from a jailed child", before - env.size(),
				before);
	}
}
