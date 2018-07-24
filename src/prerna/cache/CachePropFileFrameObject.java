package prerna.cache;

public class CachePropFileFrameObject {
	
	private String frameFileLocation;
	private String frameMetaFileLocation;
	
	private String frameName;
	private String frameType;
	
	public CachePropFileFrameObject() {
	
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
