package prerna.engine.impl.model.message;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import prerna.date.SemossDate;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.Feedback;
import prerna.engine.impl.model.Room;

public abstract class AbstractMessage {

	protected String modelId;
	protected ModelTypeEnum modelType;
	protected String messageId;
	protected String transactionId;
	protected String parentMessageId;
	protected Feedback feedback;
	protected int tokens;
	
    protected boolean visible = true;
	protected transient Room room;

    private SemossDate dateCreated;
    
    @SerializedName("ornaments")
    protected Map<String, Object> ornaments = new HashMap<>();

	public AbstractMessage() {
		this.messageId = UUID.randomUUID().toString();
        this.dateCreated = new SemossDate(ZonedDateTime.now(ZoneOffset.UTC));
	}

	public abstract MessageType getMessageType();

	// this should really never be used unless we are translating old message formats
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
	};

	public ModelTypeEnum getModelType() {
		return this.modelType;
	};

	public String toJson() {
		Gson gson = new Gson();
		// This will use Gson's defaults for fields; if you want to control the exact
		// keys/structure,
		// See the "Custom Map" below.
		return gson.toJson(this);
	}

	public String getParentMessageId() {
		return parentMessageId;
	}

	public void setParentMessageId(String parentMessageId) {
		this.parentMessageId = parentMessageId;
	}

	public Feedback getFeedback() {
		return feedback;
	}

	public void setFeedback(Feedback feedback) {
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

	public SemossDate getDateCreated() {
		return dateCreated;
	}
	
	//ONLY TO BE USED FOR UPDATED LEGACY MESSAGES.
	public void setDateCreated(SemossDate dateCreated) {
		this.dateCreated = dateCreated;
	}

	public int getTokensInMessage() {
		return tokens;
	}

	public void setTokensInMessage(int tokens) {
		this.tokens=tokens;
	}

    // ----------- Ornaments -----------
    public Map<String, Object> getOrnaments() {
        return new HashMap<>(ornaments);
    }

    public void setOrnament(String key, Object value) {
        if (ornaments == null) ornaments = new HashMap<>();
        ornaments.put(key, value);
    }
    public Object getOrnament(String key) {
        return ornaments != null ? ornaments.get(key) : null;
    }
}
