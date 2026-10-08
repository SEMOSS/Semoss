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
package prerna.io.connector.mail;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.auth.User;

/**
 * What the reactors that send by way of a draft have in common: replying,
 * forwarding, and sending a draft already saved.
 *
 * <p>
 * A reply or a forward is always written as a draft first, which is what lets
 * one carry html, attachments and a chosen set of recipients in every mailbox,
 * and is then either left in Drafts or sent. Sending is the same call whichever
 * reactor asked for it, and passes through
 * {@link #deliver(ComposedMail, java.util.concurrent.Callable)} so it is
 * recorded.
 * </p>
 */
public abstract class AbstractDraftDeliveryReactor extends AbstractMailReactor {

	/**
	 * Send a draft that is saved in the user's mailbox.
	 *
	 * @param user  the signed in user
	 * @param draft the draft, as saved
	 * @return the message as sent, under whatever ids the provider gave it, since a
	 *         sent draft leaves Drafts
	 * @throws Exception when the draft cannot be sent
	 */
	protected abstract ComposedMail sendDraft(User user, ComposedMail draft) throws Exception;

	/**
	 * Send the draft, or leave it in Drafts, and say which.
	 *
	 * @param draft the draft, as saved
	 * @param send  whether it is sent rather than left for somebody to send
	 * @return what the reactor answers with
	 * @throws Exception when the draft cannot be sent
	 */
	protected final Map<String, Object> finish(ComposedMail draft, boolean send) throws Exception {
		Map<String, Object> output = new LinkedHashMap<>();
		if (!send) {
			output.put("draft", true);
			output.putAll(draft.toMap());
			return output;
		}
		User user = this.insight.getUser();
		ComposedMail sent = deliver(draft, () -> sendDraft(user, draft));
		output.put("sent", true);
		output.putAll(sent.toMap());
		return output;
	}
}
