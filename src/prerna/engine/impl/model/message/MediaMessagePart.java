package prerna.engine.impl.model.message;

import com.google.gson.annotations.SerializedName;

public class MediaMessagePart extends MessagePart {

	@SerializedName("mediaInfo")
	private MessageInputMedia mediaInfo;

	public MediaMessagePart() {
		super(MessagePartType.MEDIA);
	}

	public MediaMessagePart(MessageInputMedia mediaInfo) {
		this();
		this.mediaInfo = mediaInfo;
	}

	public MessageInputMedia getMediaInfo() {
		return mediaInfo;
	}

	public void setMediaInfo(MessageInputMedia mediaInfo) {
		this.mediaInfo = mediaInfo;
	}
}

