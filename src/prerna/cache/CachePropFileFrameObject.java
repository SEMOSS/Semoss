package prerna.cache;

public class CachePropFileFrameObject {
	
	// the cache location of the frame
	private String frameCacheLocation;
	// the cache location of the metadata
	private String frameMetaCacheLocation;
	// the cache location of frame state variables
	private String frameStateCacheLocation;
	
	// the frame name
	private String frameName;
	// the frame type
	private String frameType;
	
	public CachePropFileFrameObject() {
	
	}

	public String getFrameCacheLocation() {
		return frameCacheLocation;
	}

	public void setFrameCacheLocation(String frameCacheLocation) {
		this.frameCacheLocation = frameCacheLocation;
	}

	public String getFrameMetaCacheLocation() {
		return frameMetaCacheLocation;
	}

	public void setFrameMetaCacheLocation(String frameMetaCacheLocation) {
		this.frameMetaCacheLocation = frameMetaCacheLocation;
	}

	public String getFrameStateCacheLocation() {
		return frameStateCacheLocation;
	}

	public void setFrameStateCacheLocation(String frameStateCacheLocation) {
		this.frameStateCacheLocation = frameStateCacheLocation;
	}

	public String getFrameName() {
		return frameName;
	}

	public void setFrameName(String frameName) {
		this.frameName = frameName;
	}

	public String getFrameType() {
		return frameType;
	}

	public void setFrameType(String frameType) {
		this.frameType = frameType;
	}

	
	
}
