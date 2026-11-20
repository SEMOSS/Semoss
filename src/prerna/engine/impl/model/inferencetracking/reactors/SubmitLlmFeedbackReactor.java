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
package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.List;

import prerna.auth.User;
import prerna.engine.impl.model.MessageFeedback;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.MessageIO;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SubmitLlmFeedbackReactor extends AbstractReactor {

	public SubmitLlmFeedbackReactor() {
		this.keysToGet = new String[] { "roomId", "messageId", "feedbackText", "rating" };
		this.keyRequired = new int[] { 1, 1, 0, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String roomId = this.keyValue.get(this.keysToGet[0]);
		String messageId = this.keyValue.get(this.keysToGet[1]);
		String feedbackText = this.keyValue.get(this.keysToGet[2]);
		Boolean inRating = null;
		if ("true".equalsIgnoreCase(keyValue.get(keysToGet[3]))) {
			inRating = Boolean.TRUE;
		} else if ("false".equalsIgnoreCase(keyValue.get(keysToGet[3]))) {
			inRating = Boolean.FALSE;
		}
		final Boolean rating = inRating;

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		// boolean output =
		// ModelInferenceLogsUtils.userIsMessageAuthor(user.getPrimaryLoginToken().getId(),
		// messageId);
		// if (!output) {
		// throw new SemossPixelException("User is not the author of this message and
		// cannot provide feedback");
		// }

		try {
			// Load room
			Room room = RoomUtils.getOrLoadRoom(roomId, insight);

			if (!room.isMessageAuthor(messageId)) {
				throw new SemossPixelException(
						"User is not the author of this message and cannot provide feedback");
			}

			// Get messages
			List<AbstractMessage> messagesList = room.getMessages();

			// Find the target message
			AbstractMessage targetMessage = messagesList.parallelStream()
					.filter(msg -> msg.getMessageId().equals(messageId))
					.findFirst()
					.orElseThrow(() -> new SemossPixelException("Message not found with id: " + messageId));

			// Validate the message is a response
			if (targetMessage.getIo() != MessageIO.OUTPUT) {
				throw new SemossPixelException("Feedback can only be submitted on response messages");
			}

			// Create feedback object
			MessageFeedback feedback = new MessageFeedback(messageId, feedbackText, rating);

			// Add feedback to message
			if (rating != null) {
				targetMessage.setFeedback(feedback);
			} else {
				targetMessage.setFeedback(null);
			}

			// Flush messages to db
			RoomMessageStore.persist(room, insight.getUser().getPrimaryLoginToken().getId());

			// Now can add to the feedback table. If neither true or false was parsed,
			// remove from db
			if (rating != null) {
				ModelInferenceLogsUtils.recordFeedback(feedback);
			} else {
				ModelInferenceLogsUtils.removeFeedback(messageId);
			}
		} catch (Exception e) {
			throw new IllegalArgumentException("Unable to record the feedback: " + e.getMessage());
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("messageId")) {
			return "The unique identififer for the I/O betweeen a user and the LLM response";
		} else if (key.equals("feedbackText")) {
			return "Additional feedback in the form of text to decribe the issue/benefits of the response";
		} else if (key.equals("rating")) {
			return "true/false value to indicate if the reponse was helpful or not, or null if not parsed correctly/null";
		} else if (key.equals("roomId")) {
			return "The room into which to add this feedback";
		}
		return super.getDescriptionForKey(key);
	}
}
