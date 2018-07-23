package prerna.cache;

public class CachePropFileFrameObject {
	
	private String frameName;
	private String frameFileLocation;
	private String frameMetaFileLocation;
	private String frameType;
	
	public CachePropFileFrameObject() {
	
	}
	
	public CachePropFileFrameObject(String frameName, String frameFileLocation, String frameMetaFileLocation) {
		this.frameName = frameName;
		this.frameFileLocation = frameFileLocation;
		this.frameMetaFileLocation = frameMetaFileLocation;
	}
	
	public String getFrameName() {
		return frameName;
	}
	
	public String getFrameFileLocation() {
		return frameFileLocation;
	}
	
	public String getFrameMetaLocation() {
		return frameMetaFileLocation;
	}
	
	public String getFrameType() {
		return frameType;
	}

	public void setFrameName(String frameName) {
		this.frameName = frameName;
	}
	
	public void setFrameFileLocation(String frameFileLocation) {
		this.frameFileLocation = frameFileLocation;
	}
	
	public void setFrameMetaLocation(String frameMetaLocation) {
		this.frameMetaFileLocation = frameMetaLocation;
	}
	
	public void setFrameType(String frameType) {
		this.frameType = frameType;
	}
}
