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
package prerna.engine.impl.function.mail.model;

import java.util.ArrayList;
import java.util.List;

/**
 * What changing a mailbox did.
 *
 * <p>
 * The ids are the ones that are valid afterwards, which is not always the ones
 * that went in: a move is a copy and a delete on the server, so the message
 * that comes back is not the one that was asked about. A caller that wants to
 * touch the same message again has to use these.
 *
 * @param affected   how many messages were changed
 * @param messageIds the ids the messages have after the change
 */
public record MailboxActionResult(int affected, List<Object> messageIds) {

	public MailboxActionResult {
		messageIds = List.copyOf(messageIds);
	}

	/**
	 * Build a result from ids of whatever type the mailbox names messages with,
	 * which is a number over the protocols and a string over Graph.
	 *
	 * @param affected how many messages were changed
	 * @param ids      the ids they have afterwards
	 * @return the result
	 */
	public static MailboxActionResult of(int affected, List<?> ids) {
		return new MailboxActionResult(affected, new ArrayList<>(ids));
	}
}
