package prerna.engine.impl.model.message;

import com.google.gson.annotations.SerializedName;

/**
 * Base type for parts-based messages.
 * <p>
 * Concrete subclasses are type-specific (text/media/tool/etc.) to avoid mixing
 * unrelated fields.
 */
public abstract class MessagePart {

	@SerializedName("type")
	protected MessagePartType type = MessagePartType.UNKNOWN;

	protected MessagePart() {
	}

	protected MessagePart(MessagePartType type) {
		this.type = type == null ? MessagePartType.UNKNOWN : type;
	}

	public MessagePartType getType() {
		return type;
	}
}

