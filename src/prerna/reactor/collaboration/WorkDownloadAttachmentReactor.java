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
package prerna.reactor.collaboration;

import prerna.auth.User;
import prerna.collaboration.BrainAttachments;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkDownloadAttachment(threadId=["..."], messageId=["..."], attachmentName=["report.xlsx"], includeText=[true]);
// The UI names the file by attachmentId instead.
public class WorkDownloadAttachmentReactor extends AbstractCollaborationReactor {

	private static final String THREAD_ID = "threadId";
	private static final String MESSAGE_ID = "messageId";
	private static final String ATTACHMENT_ID = "attachmentId";
	private static final String ATTACHMENT_NAME = "attachmentName";
	private static final String FILE_NAME = "fileName";
	private static final String INCLUDE_TEXT = "includeText";

	public WorkDownloadAttachmentReactor() {
		this.keysToGet = new String[] { THREAD_ID, MESSAGE_ID, ATTACHMENT_NAME, ATTACHMENT_ID, FILE_NAME, INCLUDE_TEXT };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String threadId = getString(THREAD_ID);
		String messageId = getString(MESSAGE_ID);
		String attachmentId = getString(ATTACHMENT_ID);
		String attachmentName = getString(ATTACHMENT_NAME);
		if (threadId == null || messageId == null || (attachmentId == null && attachmentName == null)) {
			throw new IllegalArgumentException("Must pass a threadId, messageId, and attachmentName");
		}
		return mapResult(BrainAttachments.stage(user, this.insight.getInsightFolder(), threadId, messageId,
				attachmentId, attachmentName, getString(FILE_NAME), Boolean.TRUE.equals(getBoolean(INCLUDE_TEXT))));
	}

	@Override
	public String getReactorDescription() {
		return "Downloads one file attached to an email in this thread into the working directory, so it can be read "
				+ "with the file tools. Downloads only the one attachment named, only while the thread's rules still show "
				+ "its email. Use it when the owner asks about an attached file; each email in the thread lists the names of "
				+ "its attachments. The result gives the file's path in the working directory (and a .txt path for Word, Excel, "
				+ "PowerPoint, .msg and .eml files when includeText is true).";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (THREAD_ID.equals(key)) {
			return "Id of the thread the email is on";
		}
		if (MESSAGE_ID.equals(key)) {
			return "Id of the email that carries the attachment, as listed in the thread's messages";
		}
		if (ATTACHMENT_NAME.equals(key)) {
			return "File name of the attachment, as listed on that email, for example report.xlsx";
		}
		if (ATTACHMENT_ID.equals(key)) {
			return "Id of the attachment, instead of its name; the app passes this, you pass attachmentName";
		}
		if (FILE_NAME.equals(key)) {
			return "Name to save the file as in the working directory; the attachment's own name when omitted";
		}
		if (INCLUDE_TEXT.equals(key)) {
			return "Also write <file>.txt with the text of a Word, Excel, PowerPoint, .msg, or .eml file; defaults to false";
		}
		return super.getDescriptionForKey(key);
	}
}
