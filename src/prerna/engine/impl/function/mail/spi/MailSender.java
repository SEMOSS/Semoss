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
package prerna.engine.impl.function.mail.spi;

import java.util.Properties;

import prerna.engine.impl.function.mail.model.OutboundMail;
import prerna.engine.impl.function.mail.model.SendResult;

/**
 * How a message actually leaves the instance.
 *
 * <p>
 * The sending engine decides what may be sent and to whom; an implementation of
 * this decides how. That split is what lets the same engine relay through SMTP
 * or post to Microsoft Graph without the send policy, the published parameters
 * or the answer shape depending on which one it got.
 *
 * <p>
 * By the time an implementation sees a message it has already been checked, so
 * it is not the place to enforce anything. Its job is to deliver what it was
 * given and say whether that worked.
 */
public interface MailSender extends AutoCloseable {

	/**
	 * Prepare to send, once, when the engine opens.
	 *
	 * <p>
	 * Credentials are validated here rather than on the first send, so an engine
	 * that was configured wrongly fails while somebody is still looking at it.
	 *
	 * @param properties the engine's SMSS properties
	 * @throws Exception when the properties do not describe a usable mail server
	 */
	void open(Properties properties) throws Exception;

	/**
	 * Deliver one message.
	 *
	 * <p>
	 * A refusal is reported rather than thrown, since a failed send is an ordinary
	 * outcome that the caller records either way.
	 *
	 * @param message the message to send
	 * @return whether it was delivered, and the address it went out as
	 */
	SendResult send(OutboundMail message);

	/**
	 * @return what this sends through, for the line logged when a connection opens
	 */
	String describe();

	/**
	 * What to check when a send is refused.
	 *
	 * <p>
	 * Appended to the error a caller sees, because the cause is usually in the mail
	 * server's configuration rather than in the message, and the two are told apart
	 * by things only the implementation knows.
	 *
	 * @return the hint, or null when there is nothing useful to add
	 */
	default String failureHint() {
		return null;
	}

	@Override
	default void close() {
		// most adapters hold no live connection between sends, so there is nothing
		// to release. one that does overrides this
	}
}
