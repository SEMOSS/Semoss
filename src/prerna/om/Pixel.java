package prerna.om;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class Pixel {

	private String id = null;
	private String pixelString = null;

	// some state management when editing the recipe
	private boolean isMeta = false;
	private boolean returnedError = false;
	private boolean returnedWarning = false;

	// some additional metadata to maintain on the Pixel
	private Map<String, Map<String, Object>> startingFrameHeaders = new HashMap<>();
	private Map<String, Map<String, Object>> endingFrameHeaders = new HashMap<>();
	// the list of reactor inputs
	private List<Map<String, List<Map>>> reactorInputs = new Vector<>();
	// store the list of frame inputs
	private Set<String> frameInputs = new HashSet<>();
	// store the list of frame outputs from the reactor
	private Set<String> frameOutputs = new HashSet<>();
	
	// for the FE view
	private Map<String, Object> positionMap = new HashMap<>();
	
	// currently unused - just thinking of things to store
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
	 * Add the frame output from the pixel
	 * @param frameName
	 */
	public void addFrameOutput(String frameName) {
		this.frameOutputs.add(frameName);
	}
	
	/**
	 * Get the frame outputs
	 * @return
	 */
	public Set<String> getFrameOutputs() {
		return frameOutputs;
	}
	
	/**
	 * Set the frame outputs
	 * @param frameOutputs
	 */
	public void setFrameOutputs(Set<String> frameOutputs) {
		this.frameOutputs = frameOutputs;
	}
	
	/**
	 * Add the frame input from the pixel
	 * @param frameName
	 */
	public void addFrameInput(String frameName) {
		this.frameInputs.add(frameName);
	}
	
	/**
	 * Get the frame inputs
	 * @return
	 */
	public Set<String> getFrameInputs() {
		return frameInputs;
	}
	
	/**
	 * Set the frame inputs
	 * @param frameInputs
	 */
	public void setFrameInputs(Set<String> frameInputs) {
		this.frameInputs = frameInputs;
	}
	
	/**
	 * Get the position map
	 * @return
	 */
	public Map<String, Object> getPositionMap() {
		return positionMap;
	}

	/**
	 * Set the position map
	 * @param positionMap
	 */
	public void setPositionMap(Map<String, Object> positionMap) {
		this.positionMap = positionMap;
	}
	
	/**
	 * Get if this is meta or not
	 * @return
	 */
	public boolean isMeta() {
		return isMeta;
	}

	/**
	 * Set if the pixel is meta or not
	 * @param isMeta
	 */
	public void setMeta(boolean isMeta) {
		this.isMeta = isMeta;
	}
	
	/**
	 * Get if this pixel returned an error during execution
	 * @return
	 */
	public boolean isReturnedError() {
		return returnedError;
	}

	/**
	 * Set if this pixel returned an error during execution
	 * @param returnedError
	 */
	public void setReturnedError(boolean returnedError) {
		this.returnedError = returnedError;
	}

	/**
	 * Get if this pixel returned a warning during execution
	 * @return
	 */
	public boolean isReturnedWarning() {
		return returnedWarning;
	}

	/**
	 * Set if this pixel returned a warning during execution
	 * @param returnedWarning
	 */
	public void setReturnedWarning(boolean returnedWarning) {
		this.returnedWarning = returnedWarning;
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
			pixelObj.setFrameInputs(mergePixel.getFrameInputs());
			pixelObj.setFrameOutputs(mergePixel.getFrameOutputs());
		}
	}
	
	//////////////////////////////////////////
	
	// currently unused - just thinking of things to store

	public boolean isParamSelection() {
		return isParamSelection;
	}

	public void setParamSelection(boolean isParamSelection) {
		this.isParamSelection = isParamSelection;
	}
	
}
