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
package prerna.engine.impl.function.mail;

import java.util.Properties;

/**
 * How a {@link SMTPFunctionEngine} actually gets a message out.
 *
 * <p>
 * Everything the SMSS limits about a message - who it can be from, who it can
 * go to, how many at once, whether it can carry files - is settled by the
 * engine before a transport is asked to do anything, so an implementation is
 * only the delivery. That is what makes the two interchangeable behind one
 * engine and one catalog entry: a mailbox can move from SMTP to Graph by
 * changing one setting, and nothing about what the engine is allowed to send
 * changes with it.
 */
public interface MailTransport {

	/**
	 * Read whatever this transport needs out of the engine properties.
	 *
	 * @param smssProp the engine properties
	 * @throws Exception when the properties do not describe a usable mail service
	 */
	void open(Properties smssProp) throws Exception;

	/**
	 * Send one message that has already been checked against the engine's limits.
	 *
	 * @param to          the to recipients, or null when there are none
	 * @param cc          the cc recipients, or null when there are none
	 * @param bcc         the bcc recipients, or null when there are none
	 * @param from        the sender, which may carry a display name
	 * @param subject     the subject line
	 * @param message     the body of the email
	 * @param html        whether the body is html rather than plain text
	 * @param attachments the file paths to attach, or null when there are none
	 * @return true when the message was handed off
	 */
	boolean send(String[] to, String[] cc, String[] bcc, String from, String subject, String message, boolean html,
			String[] attachments);

	/**
	 * Where this transport sends, for the log line that records a send.
	 *
	 * @return the mail service
	 */
	String describe();

	/**
	 * What to check when a send fails in a way particular to this transport.
	 *
	 * @return the sentence to append to the error, or null when there is nothing to
	 *         add
	 */
	default String failureHint() {
		return null;
	}

	/**
	 * Release anything held open between sends.
	 */
	default void close() {
		// neither transport holds a connection between sends
	}

}
