package prerna.om;

import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;

public class Pixel {

	private String uid = null;
	private String pixelString = null;
	
	// some state management when editing the recipe
	private boolean hasChanged = false;

	// some additional metadata to maintain on the Pixel
	private transient ITableDataFrame primaryFrame;
	private Map<String, Object> frameHeaders;
	private boolean isParamSelection = false;

	/**
	 * Pixel component requires a uid and the pixel string
	 * @param uid
	 * @param pixelString
	 */
	public Pixel(String uid, String pixelString) {
		this.uid = uid;
		this.pixelString = pixelString;
	}
	
	/**
	 * Get the pixel string
	 * @return
	 */
	public String getPixelString() {
		return this.pixelString;
	}
	
	public String getUid() {
		return this.uid;
	}
	
	/**
	 * Modify the pixel string
	 * @param pixelString
	 */
	public void modifyPixelString(String pixelString) {
		this.pixelString = pixelString;
		this.hasChanged = true;
	}
	
	public boolean isHasChanged() {
		return hasChanged;
	}

	public void setHasChanged(boolean hasChanged) {
		this.hasChanged = hasChanged;
	}

	public ITableDataFrame getPrimaryFrame() {
		return primaryFrame;
	}

	public void setPrimaryFrame(ITableDataFrame primaryFrame) {
		this.primaryFrame = primaryFrame;
	}

	public Map<String, Object> getFrameHeaders() {
		return frameHeaders;
	}

	public void setFrameHeaders(Map<String, Object> frameHeaders) {
		this.frameHeaders = frameHeaders;
	}

	public boolean isParamSelection() {
		return isParamSelection;
	}

	public void setParamSelection(boolean isParamSelection) {
		this.isParamSelection = isParamSelection;
	}
	
}
