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

// BrainStageAttachment(threadId=["..."], messageId=["..."], attachmentId=["..."], fileName=["..."], includeText=[true]);
public class BrainStageAttachmentReactor extends AbstractCollaborationReactor {

	private static final String THREAD_ID = "threadId";
	private static final String MESSAGE_ID = "messageId";
	private static final String ATTACHMENT_ID = "attachmentId";
	private static final String FILE_NAME = "fileName";
	private static final String INCLUDE_TEXT = "includeText";

	public BrainStageAttachmentReactor() {
		this.keysToGet = new String[] { THREAD_ID, MESSAGE_ID, ATTACHMENT_ID, FILE_NAME, INCLUDE_TEXT };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String threadId = getString(THREAD_ID);
		String messageId = getString(MESSAGE_ID);
		String attachmentId = getString(ATTACHMENT_ID);
		if (threadId == null || messageId == null || attachmentId == null) {
			throw new IllegalArgumentException("Must pass a threadId, messageId, and attachmentId");
		}
		return mapResult(BrainAttachments.stage(user, this.insight.getInsightFolder(), threadId, messageId,
				attachmentId, getString(FILE_NAME), Boolean.TRUE.equals(getBoolean(INCLUDE_TEXT))));
	}

	@Override
	public String getReactorDescription() {
		return "Copies one file attached to an email on a Brain thread into this insight's folder, only while the "
				+ "thread's rules still show that email; optionally with a plain-text copy of an Office or mail file";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (THREAD_ID.equals(key)) {
			return "Thread id";
		}
		if (MESSAGE_ID.equals(key)) {
			return "Id of the email on that thread, as BrainGetThreadMessages returned it";
		}
		if (ATTACHMENT_ID.equals(key)) {
			return "Id of the attachment, as BrainGetThreadMessages listed it";
		}
		if (FILE_NAME.equals(key)) {
			return "Name to save the file as in the insight folder; the attachment's own name when omitted";
		}
		if (INCLUDE_TEXT.equals(key)) {
			return "Also write <file>.txt with the text of a Word, Excel, PowerPoint, .msg, or .eml file; defaults to false";
		}
		return super.getDescriptionForKey(key);
	}
}
