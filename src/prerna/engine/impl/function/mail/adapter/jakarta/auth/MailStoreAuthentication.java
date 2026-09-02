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
package prerna.engine.impl.function.mail.adapter.jakarta.auth;

import java.util.Properties;

import jakarta.mail.PasswordAuthentication;

/**
 * How a Jakarta mailbox store signs in.
 *
 * <p>
 * A mailbox password and a Microsoft OAuth token reach jakarta.mail the same
 * way, as the password argument to {@code Store.connect}, and differ in
 * everything around that: a token needs the session told to use XOAUTH2, is
 * fetched again for every connect because it expires, and is worth retrying
 * once when it is refused. Keeping that behind one interface is what lets the
 * store clients hold a single connect path rather than branching on which kind
 * of credential they were given.
 */
public interface MailStoreAuthentication {

	/**
	 * Add whatever jakarta.mail properties this way of signing in needs.
	 *
	 * <p>
	 * Called while the session is being built, before the raw {@code mail.} keys
	 * from the SMSS, so a caller can still override any of them.
	 *
	 * @param mailProperties the session properties being built
	 * @param protocol       the protocol the connection ended up on, such as
	 *                       {@code imaps}
	 */
	default void configure(Properties mailProperties, String protocol) {
		// a password needs nothing beyond the credentials themselves
	}

	/**
	 * The secret to connect with.
	 *
	 * <p>
	 * Read on every connect rather than held, so a credential that expires can hand
	 * over a current one.
	 *
	 * @return the password or token
	 */
	String connectSecret();

	/**
	 * @return what this signs in with, for the line logged when a connection opens
	 */
	String description();

	/**
	 * The credentials for the session's own authenticator, which jakarta.mail
	 * consults separately from {@code Store.connect}.
	 *
	 * @param username the mailbox being signed in to
	 * @return the credentials, or null to leave the session without an
	 *         authenticator
	 */
	default PasswordAuthentication sessionAuthentication(String username) {
		return null;
	}

	/**
	 * Whether a refused sign in is worth one more attempt with a new credential.
	 *
	 * <p>
	 * True for a token, which was valid when it was issued and can stop being valid
	 * before its stated expiry. False for a password out of an SMSS, which will not
	 * be any different the second time.
	 *
	 * @return true when there is a new credential worth retrying with
	 */
	default boolean refreshAfterRejection() {
		return false;
	}

	/**
	 * What to check when the mail server issues no complaint beyond refusing.
	 *
	 * @return the hint, or null when there is nothing useful to add
	 */
	default String failureHint() {
		return null;
	}

	/**
	 * What the credential itself says, for the log when it is refused.
	 *
	 * @return the description, or null when the credential says nothing useful
	 */
	default String diagnostic() {
		return null;
	}
}
