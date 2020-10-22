package prerna.om;

import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;

public class Pixel {

	private String id = null;
	private String pixelString = null;
	
	// some state management when editing the recipe
	private boolean hasChanged = false;

	// some additional metadata to maintain on the Pixel
	private Map<String, Map<String, Object>> startingFrameHeaders;
	private Map<String, Map<String, Object>> endingFrameHeaders;
	private Map<String, List<Map>> reactorInput;
	
	
	// currently unused - just thinking of things to store
	private transient ITableDataFrame primaryFrame;
	private boolean isParamSelection = false;

	/**
	 * Pixel component requires a id and the pixel string
	 * @param id
	 * @param pixelString
	 */
	public Pixel(String id, String pixelString) {
		this.id = id;
		this.pixelString = pixelString;
	}
	
	/**
	 * Get the pixel string
	 * @return
	 */
	public String getPixelString() {
		return this.pixelString;
	}
	
	/**
	 * Set the id for the pixel step
	 * @param id
	 */
	public void setId(String id) {
		this.id = id;
	}
	
	/**
	 * Grab the id
	 * @return
	 */
	public String getId() {
		return this.id;
	}
	
	/**
	 * Modify the pixel string
	 * @param pixelString
	 */
	public void modifyPixelString(String pixelString) {
		this.pixelString = pixelString;
		this.hasChanged = true;
	}
	
	public Map<String, Map<String, Object>> getStartingFrameHeaders() {
		return startingFrameHeaders;
	}

	public void setStartingFrameHeaders(Map<String, Map<String, Object>> startingFrameHeaders) {
		this.startingFrameHeaders = startingFrameHeaders;
	}
	
	public Map<String, Map<String, Object>> getEndingFrameHeaders() {
		return endingFrameHeaders;
	}

	public void setEndingFrameHeaders(Map<String, Map<String, Object>> endingFrameHeaders) {
		this.endingFrameHeaders = endingFrameHeaders;
	}
	
	public Map<String, List<Map>> getReactorInput() {
		return reactorInput;
	}

	public void setReactorInput(Map<String, List<Map>> reactorInput) {
		this.reactorInput = reactorInput;
	}
	
	//////////////////////////////////////////
	
	// currently unused - just thinking of things to store

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


	public boolean isParamSelection() {
		return isParamSelection;
	}

	public void setParamSelection(boolean isParamSelection) {
		this.isParamSelection = isParamSelection;
	}
	
}
