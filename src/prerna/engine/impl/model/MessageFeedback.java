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
package prerna.engine.impl.model;

import prerna.date.SemossDate;
import prerna.util.Utility;

public class MessageFeedback {
	private String messageId;
	private String feedbackText;
	private SemossDate feedbackDate;
	private Boolean rating;

	public MessageFeedback(String messageId, String feedbackText, SemossDate feedbackDate, Boolean rating) {
		this.messageId = messageId;
		this.feedbackText = feedbackText;
		this.feedbackDate = feedbackDate;
		this.rating = rating == null ? null : rating.booleanValue();
	}

	public MessageFeedback(String messageId, String feedbackText, Boolean rating) {
		this.messageId = messageId;
		this.feedbackText = feedbackText;
		this.feedbackDate = new SemossDate(Utility.getCurrentZonedDateTimeUTC());
		this.rating = rating == null ? null : rating.booleanValue();
	}

	public MessageFeedback() {

	}

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public String getFeedbackText() {
		return feedbackText;
	}

	public void setFeedbackText(String feedbackText) {
		this.feedbackText = feedbackText;
	}

	public SemossDate getFeedbackDate() {
		return feedbackDate;
	}

	public void setFeedbackDate(SemossDate feedbackDate) {
		this.feedbackDate = feedbackDate;
	}

	public boolean getRating() {
		return rating;
	}

	public void setRating(boolean rating) {
		this.rating = rating;
	}
}
