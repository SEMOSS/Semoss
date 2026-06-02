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
package prerna.engine.impl.model.message;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.annotations.SerializedName;

import prerna.date.SemossDate;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.MessageFeedback;
import prerna.engine.impl.model.Room;

public abstract class AbstractMessage {
	/**
	 * Latest supported message JSON schema version for persisted room messages.
	 */
	public static final int LATEST_SCHEMA_VERSION = 2;

	/**
	 * Message JSON schema version.
	 * <p>
	 * Version 1 = legacy flat message fields
	 * (type/content/imageInfos/tool_responses...). Version 2 = parts-based schema
	 * via {@code parts}.
	 */
	@SerializedName("schemaVersion")
	protected Integer schemaVersion;

	/**
	 * Discriminator to support clean deserialization into InputMessage vs
	 * ResponseMessage without relying on legacy {@code type}.
	 */
	@SerializedName("io")
	protected MessageIO io;

	@SerializedName("parts")
	protected List<MessagePart> parts = new ArrayList<>();

	protected String modelId;
	protected ModelTypeEnum modelType;
	protected String messageId;
	protected String transactionId;
	protected String parentMessageId;
	protected String summaryLeafMessageId;
	protected MessageFeedback feedback;
	protected int tokens;

	// Cache token counts: cacheReadTokens on input messages, cacheCreationTokens on response messages.
	@SerializedName("cacheReadTokens")
	protected Integer cacheReadTokens;

	@SerializedName("cacheCreationTokens")
	protected Integer cacheCreationTokens;

	protected boolean visible = true;
	protected boolean pruneToolsAbove = false;

	@SerializedName("platform_generated")
	protected boolean platformGenerated = false;

	protected transient Room room;

	private SemossDate dateCreated;

	@SerializedName("ornaments")
	protected Map<String, Object> ornaments = new HashMap<>();

	public AbstractMessage() {
		this.messageId = GUID.v7().toUUID().toString();
		this.dateCreated = new SemossDate(ZonedDateTime.now(ZoneOffset.UTC));
	}

	public abstract MessageType getMessageType();

	/**
	 * Called after deserialization to ensure the message is internally consistent.
	 * Subclasses should ensure parts are hydrated from legacy fields (and
	 * vice-versa).
	 */
	public void normalizeAfterLoad(Room room) {
		if (room != null) {
			setRoom(room);
		}
	}

	/**
	 * Called before serialization to ensure the message writes in the latest
	 * format.
	 */
	public void normalizeForWrite() {
		if (schemaVersion == null || schemaVersion < LATEST_SCHEMA_VERSION) {
			schemaVersion = LATEST_SCHEMA_VERSION;
		}
	}

	public MessageIO getIo() {
		return io;
	}

	public void setIo(MessageIO io) {
		this.io = io;
	}

	public Integer getSchemaVersion() {
		return schemaVersion;
	}

	public void setSchemaVersion(Integer schemaVersion) {
		this.schemaVersion = schemaVersion;
	}

	public List<MessagePart> getParts() {
		return parts == null ? new ArrayList<>() : new ArrayList<>(parts);
	}

	public void setParts(List<MessagePart> parts) {
		this.parts = (parts == null) ? new ArrayList<>() : new ArrayList<>(parts);
	}

	public void addPart(MessagePart part) {
		if (part == null) {
			return;
		}
		if (parts == null) {
			parts = new ArrayList<>();
		}
		parts.add(part);
	}

	public boolean hasParts() {
		return parts != null && !parts.isEmpty();
	}

	public boolean hasPartType(MessagePartType type) {
		if (type == null || parts == null) {
			return false;
		}
		for (MessagePart part : parts) {
			if (part != null && type.equals(part.getType())) {
				return true;
			}
		}
		return false;
	}

	public boolean hasTextPart() {
		return hasPartType(MessagePartType.TEXT);
	}

	public boolean hasMediaPart() {
		return hasPartType(MessagePartType.MEDIA);
	}

	public boolean hasToolCallPart() {
		return hasPartType(MessagePartType.TOOL_CALL);
	}

	public boolean hasToolResultPart() {
		return hasPartType(MessagePartType.TOOL_RESULT);
	}

	public boolean getPruneToolsAbove() {
		return this.pruneToolsAbove;
	}

	public void setPruneToolsAbove(boolean pruneToolsAbove) {
		this.pruneToolsAbove = pruneToolsAbove;
	}

	public boolean hasThinkingPart() {
		return hasPartType(MessagePartType.THINKING);
	}

	// this should really never be used unless we are translating old message
	// formats
	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public String getMessageId() {
		return messageId;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transction) {
		this.transactionId = transction;
	}

	public void setModel(IModelEngine modelEngine) {
		this.modelType = modelEngine.getModelType();
		this.modelId = modelEngine.getEngineId();
		setOrnament("modelName", modelEngine.getDisplayName());
	}

	public String getModelId() {
		return modelId;
	}

	public void setModelId(String modelId) {
		this.modelId = modelId;
	}

	public void setModelType(ModelTypeEnum modelType) {
		this.modelType = modelType;
	}

	public ModelTypeEnum getModelType() {
		return this.modelType;
	}

	public String getParentMessageId() {
		return parentMessageId;
	}

	public void setParentMessageId(String parentMessageId) {
		this.parentMessageId = parentMessageId;
	}

	public String getSummaryLeafMessageId() {
		return summaryLeafMessageId;
	}

	public void setSummaryLeafMessageId(String summaryLeafMessageId) {
		this.summaryLeafMessageId = summaryLeafMessageId;
	}

	public MessageFeedback getFeedback() {
		return feedback;
	}

	public void setFeedback(MessageFeedback feedback) {
		this.feedback = feedback;
	}

	public void setRoom(Room room) {
		this.room = room;
	}

	public Room getRoom() {
		return this.room;
	}

	public boolean isVisible() {
		return visible;
	}

	public void setVisibile(boolean visibile) {
		this.visible = visibile;
	}

	public boolean isPlatformGenerated() {
		return platformGenerated;
	}

	public void setPlatformGenerated(boolean platformGenerated) {
		this.platformGenerated = platformGenerated;
	}

	public SemossDate getDateCreated() {
		return dateCreated;
	}

	// ONLY TO BE USED FOR UPDATED LEGACY MESSAGES.
	public void setDateCreated(SemossDate dateCreated) {
		this.dateCreated = dateCreated;
	}

	public int getTokensInMessage() {
		return tokens;
	}

	public void setTokensInMessage(int tokens) {
		this.tokens = tokens;
	}

	public Integer getCacheReadTokens() {
		return cacheReadTokens;
	}

	public void setCacheReadTokens(Integer cacheReadTokens) {
		this.cacheReadTokens = cacheReadTokens;
	}

	public Integer getCacheCreationTokens() {
		return cacheCreationTokens;
	}

	public void setCacheCreationTokens(Integer cacheCreationTokens) {
		this.cacheCreationTokens = cacheCreationTokens;
	}

	// ----------- Ornaments -----------
	public Map<String, Object> getOrnaments() {
		return new HashMap<>(ornaments);
	}

	public void setOrnament(String key, Object value) {
		if (ornaments == null) {
			ornaments = new HashMap<>();
		}
		ornaments.put(key, value);
	}

	public Object getOrnament(String key) {
		return ornaments != null ? ornaments.get(key) : null;
	}
}
