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
package prerna.engine.impl.function.mail.adapter.jakarta;

import jakarta.mail.PasswordAuthentication;
import prerna.engine.impl.function.mail.spi.MailStoreAuthentication;

/**
 * Signing in to a mailbox with its own password.
 *
 * <p>
 * The ordinary case, and the one that needs nothing arranged: the password out
 * of the SMSS is what is presented, it does not expire, and there is no point
 * retrying with it when a server refuses it.
 */
public final class PasswordStoreAuthentication implements MailStoreAuthentication {

	private final String password;

	/**
	 * @param password the mailbox password, or null for a server that wants none
	 */
	public PasswordStoreAuthentication(String password) {
		this.password = password;
	}

	@Override
	public String connectSecret() {
		return this.password;
	}

	@Override
	public String description() {
		return "a password";
	}

	@Override
	public PasswordAuthentication sessionAuthentication(String username) {
		// without a password there is nothing for an authenticator to answer with,
		// which is left to the session rather than answered with a null credential
		return this.password == null ? null : new PasswordAuthentication(username, this.password);
	}
}
