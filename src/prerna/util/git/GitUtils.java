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
package prerna.util.git;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.HttpException;

import prerna.security.InstallCertNow;

/**
 * Static utility methods for interacting with GitHub and for small git-related
 * helpers, including OAuth-based GitHub login (with certificate-install retry),
 * building timestamped commit messages, and identifying file types that should
 * be ignored.
 */
public class GitUtils {

	private static final Logger classLogger = LogManager.getLogger(GitUtils.class);

	private GitUtils() {

	}

	/**
	 * Logs in to GitHub using the given OAuth token, starting at attempt 1.
	 * Convenience overload that delegates to {@link #login(String, int)}.
	 *
	 * @param oAuth the GitHub OAuth token used to authenticate
	 * @return the authenticated {@link GitHub} client, or {@code null} if login did
	 *         not succeed
	 * @throws IllegalArgumentException if the credentials are rejected with an
	 *                                  {@link IOException}
	 */
	public static GitHub login(String oAuth) {
		return login(oAuth, 1);
	}

	/**
	 * Attempts to log in to GitHub using the given OAuth token, retrying on
	 * {@link HttpException} up to a fixed limit of attempts (only attempts less
	 * than 3 are tried). On an {@link HttpException} the GitHub certificate is
	 * (re)installed via {@link InstallCertNow#please(String, String, String)}, the
	 * attempt counter is incremented, and the login is retried recursively.
	 *
	 * @param oAuth   the GitHub OAuth token used to authenticate
	 * @param attempt the current attempt number; logins are only tried while this
	 *                is below 3
	 * @return the authenticated {@link GitHub} client on success, or {@code null}
	 *         if the attempt limit is reached or a retry path does not return a
	 *         client
	 * @throws IllegalArgumentException if authentication fails with an
	 *                                  {@link IOException}, indicating invalid git
	 *                                  credentials
	 */
	public static GitHub login(String oAuth, int attempt) {
		GitHub gh = null;
		if (attempt < 3) {
			classLogger.info("Attempting login {}", attempt);
			try {
				gh = GitHub.connectUsingOAuth(oAuth);
				gh.getMyself();
				return gh;
			} catch (HttpException ex) {
				classLogger.error("Failed to login to github using oAuth on attempt {}", attempt, ex);
				try {
					InstallCertNow.please("github.com", null, null);
				} catch (Exception e) {
					classLogger.error("Failed to install certificate for github.com", e);
				}
				attempt = attempt + 1;
				login(oAuth, attempt);
			} catch (IOException e) {
				classLogger.error("Failed to login to github using oAuth", e);
				throw new IllegalArgumentException("Invalid Git Credentials for username = \"" + oAuth + "\"");
			}
		}
		return gh;
	}

	/**
	 * Builds a message string by appending the current date and time to the given
	 * prefix, formatted as {@code "yyyy/MM/dd HH:mm:ss"} (typically used as a
	 * commit message).
	 *
	 * @param prefixString the text to prepend before the timestamp
	 * @return the prefix joined to the current timestamp in the form
	 *         {@code "<prefixString> : <yyyy/MM/dd HH:mm:ss>"}
	 */
	public static String getDateMessage(String prefixString) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		return prefixString + " : " + dateFormat.format(date);
	}

	/**
	 * Determines whether a file should be ignored based on its extension. Returns
	 * {@code true} if the file name ends with one of the ignored suffixes
	 * ({@code .db} or {@code .jnl}).
	 *
	 * @param fileName the file name (or path) to test
	 * @return {@code true} if the name ends with an ignored extension;
	 *         {@code false} otherwise
	 */
	public static boolean isIgnore(String fileName) {
		String[] list = new String[] { ".db", ".jnl" };
		boolean ignore = false;
		for (int igIndex = 0; igIndex < list.length && !ignore; igIndex++) {
			ignore = fileName.endsWith(list[igIndex]);
		}
		return ignore;
	}

}
