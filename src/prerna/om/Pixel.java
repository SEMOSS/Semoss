package prerna.om;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;

public class Pixel {

	private String id = null;
	private String pixelString = null;
	
	// some state management when editing the recipe
	private boolean hasChanged = false;

	// some additional metadata to maintain on the Pixel
	private Map<String, Map<String, Object>> startingFrameHeaders;
	private Map<String, Map<String, Object>> endingFrameHeaders;
	// the list of reactor inputs
	private List<Map<String, List<Map>>> reactorInputs = new Vector<>();
	// store the list of frame outputs from the reactor
	private Set<String> frameOutput = new HashSet<>();
	
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
	
	/**
	 * Get the starting frame headers
	 * @return
	 */
	public Map<String, Map<String, Object>> getStartingFrameHeaders() {
		return startingFrameHeaders;
	}

	/**
	 * Set the starting frame headers
	 * @param startingFrameHeaders
	 */
	public void setStartingFrameHeaders(Map<String, Map<String, Object>> startingFrameHeaders) {
		this.startingFrameHeaders = startingFrameHeaders;
	}
	
	/**
	 * Get the ending frame headers
	 * @return
	 */
	public Map<String, Map<String, Object>> getEndingFrameHeaders() {
		return endingFrameHeaders;
	}

	/**
	 * Set the ending frame headers
	 * @param endingFrameHeaders
	 */
	public void setEndingFrameHeaders(Map<String, Map<String, Object>> endingFrameHeaders) {
		this.endingFrameHeaders = endingFrameHeaders;
	}
	
	/**
	 * Adding reactor input
	 * @param reactorInput
	 */
	public void addReactorInput(Map<String, List<Map>> reactorInput) {
		this.reactorInputs.add(reactorInput);
	}
	
	/**
	 * Set the reactor input
	 * @param reactorInputs
	 */
	public void setReactorInputs(List<Map<String, List<Map>>> reactorInputs) {
		this.reactorInputs = reactorInputs;
	}
	
	/**
	 * Get the reactor inputs
	 * @return
	 */
	public List<Map<String, List<Map>>> getReactorInputs() {
		return this.reactorInputs;
	}
	
	/**
	 * Get the frame outputs from the pixel
	 * @param frameName
	 */
	public void addFrameOutput(String frameName) {
		this.frameOutput.add(frameName);
	}
	
	/**
	 * Get the frame outputs
	 * @return
	 */
	public Set<String> getFrameOutput() {
		return frameOutput;
	}
	
	/**
	 * Set the frame outputs
	 * @param frameOutput
	 */
	public void setFrameOutput(Set<String> frameOutput) {
		this.frameOutput = frameOutput;
	}
	
	/**
	 * To help w/ debugging
	 */
	public String toString() {
		return this.id + "__" + this.pixelString;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Pixel) {
			Pixel otherP = (Pixel) obj;
			if(otherP.id.equals(this.id)
					&& otherP.pixelString.equals(this.pixelString)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Method to use to merge the translation pixel
	 * into the pixel for the recipe
	 * @param pixelObj
	 * @param mergePixel
	 */
	public static void translationMerge(Pixel pixelObj, Pixel mergePixel) {
		if(mergePixel != null) {
			pixelObj.setStartingFrameHeaders(mergePixel.getStartingFrameHeaders());
			pixelObj.setReactorInputs(mergePixel.getReactorInputs());
			pixelObj.setFrameOutput(mergePixel.getFrameOutput());
		}
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
