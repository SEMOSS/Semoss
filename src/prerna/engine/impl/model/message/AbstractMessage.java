package prerna.engine.impl.model.message;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

import com.google.gson.Gson;

import playground.message.formatters.MessageFormatterFactory;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.om.Insight;

public abstract class AbstractMessage {


	protected String modelId;
	protected ModelTypeEnum modelType;
	protected String messageId;
	protected String transactionId;
	protected String parentMessageId;

    protected boolean visibile = true;

	protected Map<String, Object> formattedMessage;
	protected transient Insight insight;

    private String dateCreated;
    
	public AbstractMessage() {
		this.messageId = UUID.randomUUID().toString();
        this.dateCreated = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
	}

	public abstract MessageType getMessageType();

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

	public Map<String, Object> getFormattedMessage() {
		if (formattedMessage == null) {
			formattedMessage = MessageFormatterFactory.getFormatter(this.modelType).format(this);
		}
		System.out.println("KUNAL MESSAGE ::: " + formattedMessage);
		return formattedMessage;
	}

	public void setFormattedMessage(Map<String, Object> formattedMessage) {
		this.formattedMessage = formattedMessage;
	}

//	public Map<String, Object> getFormattedMessage() {
//		IMessageFormatter formatter = MessageFormatterFactory.getFormatter(getMessageType(), this.modelType);
//		return formatter.format(this);
//	}

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

	public void setInsight(Insight insight) {
		this.insight = insight;
	}

	public Insight getInsight() {
		return this.insight;
	}
	

	public boolean isVisibile() {
		return visibile;
	}

	public void setVisibile(boolean visibile) {
		this.visibile = visibile;
	}

	public String getDateCreated() {
		return dateCreated;
	}


	

}
