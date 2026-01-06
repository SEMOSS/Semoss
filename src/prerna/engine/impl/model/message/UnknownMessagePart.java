package prerna.engine.impl.model.message;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

/**
 * Forward-compatibility holder for parts whose {@code type} is unknown to this
 * server version.
 */
public class UnknownMessagePart extends MessagePart {

	@SerializedName("data")
	private Map<String, Object> data = new LinkedHashMap<>();

	public UnknownMessagePart() {
		super(MessagePartType.UNKNOWN);
	}

	public UnknownMessagePart(MessagePartType type, Map<String, Object> data) {
		super(type);
		if (data != null) {
			this.data = new LinkedHashMap<>(data);
		}
	}

	public Map<String, Object> getData() {
		return new LinkedHashMap<>(data);
	}

	public void setData(Map<String, Object> data) {
		this.data = (data == null) ? new LinkedHashMap<>() : new LinkedHashMap<>(data);
	}
}

