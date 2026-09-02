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

import java.io.IOException;
import java.util.List;

import prerna.engine.impl.function.mail.model.MailSearchRequest;
import prerna.engine.impl.function.mail.model.MailSearchResult;
import prerna.engine.impl.function.mail.model.MailboxActionResult;

/**
 * How a mailbox is actually read.
 *
 * <p>
 * IMAP, POP3 and Graph all answer this, so a reading engine states what it
 * wants and gets the same result shape back whichever one is underneath. A
 * caller cannot tell from the output which way the mailbox was read, other than
 * by the uid: Graph names a message with an opaque string where the protocols
 * use a number.
 *
 * <p>
 * Only searching is required. The three ways of changing a mailbox are optional
 * because POP3 genuinely cannot do them - it has no folders and no read state -
 * and an implementation that says so plainly is better than one that pretends
 * and quietly does nothing. Whether a change is allowed at all is a separate
 * question, decided by the engine's policy before it ever gets here.
 */
public interface MailboxClient extends AutoCloseable {

	/**
	 * Find messages.
	 *
	 * @param request what to look for, already checked against the engine's limits
	 * @return the messages found
	 */
	MailSearchResult search(MailSearchRequest request);

	/**
	 * Mark messages read or unread.
	 *
	 * @param folder     the folder holding them
	 * @param messageIds the messages to mark
	 * @param read       true to mark read, false to mark unread
	 * @return how many were marked, and their ids
	 * @throws UnsupportedOperationException when the protocol has no read state
	 */
	default MailboxActionResult mark(String folder, List<String> messageIds, boolean read) {
		throw unsupported("mark messages");
	}

	/**
	 * Move messages to another folder.
	 *
	 * @param folder            the folder holding them
	 * @param messageIds        the messages to move
	 * @param destinationFolder the folder to move them to
	 * @return how many moved, and the ids they have afterwards, which a move can
	 *         reissue
	 * @throws UnsupportedOperationException when the protocol has no folders
	 */
	default MailboxActionResult move(String folder, List<String> messageIds, String destinationFolder) {
		throw unsupported("move messages");
	}

	/**
	 * Delete messages.
	 *
	 * <p>
	 * How final that is depends on the implementation: IMAP expunges, and Graph
	 * moves the message to Deleted Items.
	 *
	 * @param folder     the folder holding them
	 * @param messageIds the messages to delete
	 * @return how many were deleted, and their ids
	 * @throws UnsupportedOperationException when the protocol cannot delete
	 */
	default MailboxActionResult delete(String folder, List<String> messageIds) {
		throw unsupported("delete messages");
	}

	/**
	 * @param operation what was asked for
	 * @return the refusal, naming the implementation so the reader knows which
	 *         mailbox could not do it
	 */
	private UnsupportedOperationException unsupported(String operation) {
		return new UnsupportedOperationException(getClass().getSimpleName() + " cannot " + operation);
	}

	@Override
	default void close() throws IOException {
		// an api backed client holds no connection between calls, so there is nothing
		// to release. a protocol one holds a store and overrides this
	}
}
