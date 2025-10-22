package prerna.engine.impl.model.message;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.annotations.SerializedName;

import prerna.date.SemossDate;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.MessageFeedback;
import prerna.engine.impl.model.Room;

public abstract class AbstractMessage {

	protected String modelId;
	protected ModelTypeEnum modelType;
	protected String messageId;
	protected String transactionId;
	protected String parentMessageId;
	protected MessageFeedback feedback;
	protected int tokens;

	protected boolean visible = true;

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
