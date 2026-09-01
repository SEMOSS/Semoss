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
package prerna.engine.impl.function.mail.engine;

import java.util.Properties;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.impl.function.mail.adapter.jakarta.JakartaPop3MailboxClient;
import prerna.engine.impl.function.mail.attachment.AttachmentStore;
import prerna.engine.impl.function.mail.config.JakartaStoreConfig;
import prerna.engine.impl.function.mail.policy.MailReadPolicy;
import prerna.engine.impl.function.mail.spi.MailStoreAuthentication;
import prerna.engine.impl.function.mail.spi.MailboxClient;

/**
 * Function engine that reads a mailbox over POP3.
 *
 * <p>
 * The least capable of these engines, and deliberately so: one inbox, no record
 * of what has been read, and nothing that can be changed. It exists for a
 * mailbox where POP3 is what is on offer, or a process that already speaks it.
 * Where IMAP is available it is the better engine, and on a Microsoft 365
 * mailbox reached through Graph the two are the same API underneath.
 */
public class POP3FunctionEngine extends AbstractMailStoreFunctionEngine {

	public static final String POP3_HOST_KEY = "POP3_" + HOST_SUFFIX;
	public static final String POP3_PORT_KEY = "POP3_" + PORT_SUFFIX;
	public static final String POP3_USERNAME_KEY = "POP3_" + USERNAME_SUFFIX;
	public static final String POP3_PASSWORD_KEY = "POP3_" + PASSWORD_SUFFIX;
	public static final String POP3_SECURITY_KEY = "POP3_" + SECURITY_SUFFIX;

	private static final String PROTOCOL = "pop3";
	private static final String SECURE_PROTOCOL = "pop3s";
	private static final String DEFAULT_SECURE_PORT = "995";
	private static final String DEFAULT_PORT = "110";

	/**
	 * Build a POP3 engine that is not in the catalog, for a caller that already
	 * holds mailbox settings and wants this engine's connection handling rather
	 * than its own.
	 *
	 * @param engineId the id to open under, used only for logging
	 * @param props    the mailbox properties
	 * @return the opened engine
	 * @throws Exception when the properties do not describe a usable mailbox
	 */
	public static POP3FunctionEngine openTransientEngine(String engineId, Properties props) throws Exception {
		POP3FunctionEngine engine = new POP3FunctionEngine();
		engine.setBasic(true);
		engine.open(transientProperties(engineId, props, "Read the email in the " + engineId + " mailbox"));
		return engine;
	}

	@Override
	protected MailboxClient createJakartaClient(JakartaStoreConfig config, MailStoreAuthentication authentication,
			MailReadPolicy policy, AttachmentStore attachmentStore) {
		return new JakartaPop3MailboxClient(config, authentication, policy, attachmentStore);
	}

	@Override
	protected String getDefaultFunctionDescription() {
		return "Read email in a POP3 mailbox. It has one inbox, no read state, and returns the newest matches.";
	}

	@Override
	protected String getProtocol() {
		return PROTOCOL;
	}

	@Override
	protected String getSecureProtocol() {
		return SECURE_PROTOCOL;
	}

	@Override
	protected String getDefaultPort(boolean secure) {
		return secure ? DEFAULT_SECURE_PORT : DEFAULT_PORT;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.POP3.name();
	}
}
