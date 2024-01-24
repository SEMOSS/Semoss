package prerna.om;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.Constants;
import prerna.util.gson.PixelAdapter;

public class Pixel {

	private static final Logger logger = LogManager.getLogger(Pixel.class);
	
	private String id = null;
	private String pixelString = null;

	// some metadata
	private String pixelAlias = null;
	private String pixelDescription = null;
	
	// some state management when editing the recipe
	private boolean isMeta = false;
	private boolean returnedError = false;
	private boolean returnedWarning = false;
	private List<String> errorMessages = new Vector<>();
	private List<String> warningMessages = new Vector<>();

	// some additional metadata to maintain on the Pixel
	private Map<String, Map<String, Object>> startingFrameHeaders = new HashMap<>();
	private Map<String, Map<String, Object>> endingFrameHeaders = new HashMap<>();
//	// the list of reactor inputs
//	private List<Map<String, List<Map>>> reactorInputs = new Vector<>();
	// store the list of frame inputs
	private Set<String> frameInputs = new HashSet<>();
	// store the list of frame outputs from the reactor
	private Set<String> frameOutputs = new HashSet<>();
	
	// to help with caching
	// store any task options that are created
	private List<TaskOptions> taskOptions = new Vector<>();
	private List<Map<String, String>> removeLayerList = new Vector<>();
	private List<Map<String, String>> cloneMapList = new Vector<>();
	
	// for the FE view
	private Map<String, Object> positionMap = new HashMap<>();
	
	// are we a refresh panel
	private boolean isRefreshPanel = false;
	// is this a code execution? r/py/java?
	private boolean isCodeExecution = false;
	private boolean isUserScript = false;
	private Variable.LANGUAGE language = null; 
	private String codeExecuted = null;
	// is this a data transformation
	private boolean isFrameTransformation = false;
	// is this an assignment
	private boolean isAssignment = false;
	// is this a file read
	private boolean isFileRead = false;
	// save in recipe
	private boolean saveDataTransformation = false;
	private boolean saveDataExport = false;
	private boolean saveVisualization = false;

	// currently unused - just thinking of things to store
	private boolean isParamSelection = false;

	private long timeToRun = -1;
	private long startTime = -1;
	private long endTime = -1;
	
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
	 * Set the pixel string
	 * @param pixelString
	 */
	public void setPixelString(String pixelString) {
		this.pixelString = pixelString;
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
//	
//	/**
//	 * Adding reactor input
//	 * @param reactorInput
//	 */
//	public void addReactorInput(Map<String, List<Map>> reactorInput) {
//		this.reactorInputs.add(reactorInput);
//	}
//	
//	/**
//	 * Set the reactor input
//	 * @param reactorInputs
//	 */
//	public void setReactorInputs(List<Map<String, List<Map>>> reactorInputs) {
//		this.reactorInputs = reactorInputs;
//	}
//	
//	/**
//	 * Get the reactor inputs
//	 * @return
//	 */
//	public List<Map<String, List<Map>>> getReactorInputs() {
//		return this.reactorInputs;
//	}
	
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
	 * Get the task options
	 * @return
	 */
	public List<TaskOptions> getTaskOptions() {
		return taskOptions;
	}
	
	/**
	 * Add to the task options list
	 * @param taskOptions
	 */
	public void addTaskOptions(TaskOptions taskOptions) {
		this.taskOptions.add(taskOptions);
	}

	/**
	 * Set the task options
	 * @param taskOptions
	 */
	public void setTaskOptions(List<TaskOptions> taskOptions) {
		this.taskOptions = taskOptions;
	}

	/**
	 * Get the remove layer list
	 * @return
	 */
	public List<Map<String, String>> getRemoveLayerList() {
		return this.removeLayerList;
	}
	
	/**
	 * Add to the remove layer list
	 * @param removeLayer
	 */
	public void addRemoveLayer(Map<String, String> removeLayer) {
		this.removeLayerList.add(removeLayer);
	}

	/**
	 * Set the remove layer list
	 * @param removeLayerList
	 */
	public void setRemoveLayerList(List<Map<String, String>> removeLayerList) {
		this.removeLayerList = removeLayerList;
	}
	
	/**
	 * Get the clone map list
	 * @return
	 */
	public List<Map<String, String>> getCloneMapList() {
		return this.cloneMapList;
	}
	
	/**
	 * Add to the clone map list
	 * @param cloneMap
	 */
	public void addCloneMap(Map<String, String> cloneMap) {
		this.cloneMapList.add(cloneMap);
	}

	/**
	 * Set the clone map list
	 * @param cloneMapList
	 */
	public void setCloneMapList(List<Map<String, String>> cloneMapList) {
		this.cloneMapList = cloneMapList;
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
	 * Is this pixel a refresh panel task
	 * Important for being able to determine if this is the last pixel on the frame
	 * That we need to grab the original pixel that was used for painting
	 * @return
	 */
	public boolean isRefreshPanel() {
		return isRefreshPanel;
	}

	/**
	 * Set if this pixel is a refresh panel task
	 * Important for being able to determine if this is the last pixel on the frame
	 * That we need to grab the original pixel that was used for painting
	 * @return
	 */
	public void setRefreshPanel(boolean isRefreshPanel) {
		this.isRefreshPanel = isRefreshPanel;
	}
	
	/**
	 * Determine if this pixel is a known data operation
	 * @return
	 */
	public boolean isDataOperation() {
		return !this.isMeta && (
				this.isCodeExecution || 
				this.isFrameTransformation ||
				this.isAssignment ||
				this.isFileRead ||
				this.saveDataTransformation ||
				this.saveDataExport ||
				this.saveVisualization
			);
	}

	/**
	 * Is this pixel a code execution
	 * @return
	 */
	public boolean isCodeExecution() {
		return isCodeExecution;
	}
	
	/**
	 * Is this pixel code execution a user script
	 * @return
	 */
	public boolean isUserScript() {
		return isUserScript;
	}

	/**
	 * Get the language of the pixel code block
	 * @return
	 */
	public Variable.LANGUAGE getLanguage() {
		return language;
	}

	/**
	 * Get the executed code of the pixel code block
	 * @return
	 */
	public String getCodeExecuted() {
		return codeExecuted;
	}

	/**
	 * Set if this pixel is a code execution
	 * @param isCodeExecution
	 * @param codeExecuted
	 * @param language
	 */
	public void setCodeDetails(boolean isCodeExecution, String codeExecuted, Variable.LANGUAGE language, boolean isUserScript) {
		this.isCodeExecution = isCodeExecution;
		this.codeExecuted = codeExecuted;
		this.language = language;
		this.isUserScript = isUserScript;
	}

	/**
	 * Is this pixel a frame/data transformation
	 * @return
	 */
	public boolean isFrameTransformation() {
		return isFrameTransformation;
	}

	/**
	 * Set if this pixel is a frame/data transformation
	 * @param isDataTransformation
	 */
	public void setFrameTransformation(boolean isFrameTransformation) {
		this.isFrameTransformation = isFrameTransformation;
	}
	
	/**
	 * Is this pixel is an assignment
	 * @return
	 */
	public boolean isAssignment() {
		return isAssignment;
	}

	/**
	 * Set if this pixel is an assignment
	 * @param isAssignment
	 */
	public void setAssignment(boolean isAssignment) {
		this.isAssignment = isAssignment;
	}
	
	/**
	 * Get if this pixel is a file read
	 * @return
	 */
	public boolean isFileRead() {
		return isFileRead;
	}

	/**
	 * Set if this pixel is a file read
	 * @param isFileRead
	 */
	public void setFileRead(boolean isFileRead) {
		this.isFileRead = isFileRead;
	}
	
	/**
	 * Is data transformation to save in recipe
	 * @return
	 */
	public boolean isSaveDataTransformation() {
		return saveDataTransformation;
	}

	/**
	 * Set data transformation to save in recipe
	 * @param saveDataTransformation
	 */
	public void setSaveDataTransformation(boolean saveDataTransformation) {
		this.saveDataTransformation = saveDataTransformation;
	}

	/**
	 * Is data export to save in recipe
	 * @return
	 */
	public boolean isSaveDataExport() {
		return saveDataExport;
	}

	/**
	 * Set data export to save in recipe
	 * @param saveDataExport
	 */
	public void setSaveDataExport(boolean saveDataExport) {
		this.saveDataExport = saveDataExport;
	}

	/**
	 * Is data visualization to save in recipe
	 * @return
	 */
	public boolean isSaveVisualization() {
		return saveVisualization;
	}

	/**
	 * Set data visualization to save in recipe
	 * @param saveVisualization
	 */
	public void setSaveVisualization(boolean saveVisualization) {
		this.saveVisualization = saveVisualization;
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
	 * Get the error messages
	 * @return
	 */
	public List<String> getErrorMessages() {
		return errorMessages;
	}

	/**
	 * Set the error messages
	 * @param errorMessages
	 */
	public void setErrorMessages(List<String> errorMessages) {
		this.errorMessages = errorMessages;
	}
	
	/**
	 * Add an error message to the pixel
	 * @param errorMessage
	 */
	public void addErrorMessage(String errorMessage) {
		this.errorMessages.add(errorMessage);
	}

	/**
	 * Get the warning messages
	 * @return
	 */
	public List<String> getWarningMessages() {
		return warningMessages;
	}

	/**
	 * Get the warning messages
	 * @param warningMessages
	 */
	public void setWarningMessages(List<String> warningMessages) {
		this.warningMessages = warningMessages;
	}
	
	/**
	 * Add a warning message to the pixel
	 * @param warningMessage
	 */
	public void addWarningMessage(String warningMessage) {
		this.warningMessages.add(warningMessage);
	}
	
	/**
	 * Get the pixel alias
	 * @return
	 */
	public String getPixelAlias() {
		return pixelAlias;
	}

	/**
	 * Set the pixel alias
	 * @param pixelAlias
	 */
	public void setPixelAlias(String pixelAlias) {
		this.pixelAlias = pixelAlias;
	}

	/**
	 * Get the pixel description
	 * @return
	 */
	public String getPixelDescription() {
		return pixelDescription;
	}

	/**
	 * Set the pixel description
	 * @param pixelDescription
	 */
	public void setPixelDescription(String pixelDescription) {
		this.pixelDescription = pixelDescription;
	}

	/**
	 * 
	 * @return
	 */
	public long getTimeToRun() {
		return timeToRun;
	}

	/**
	 * 
	 * @param timeToRun
	 */
	public void setTimeToRun(long timeToRun) {
		this.timeToRun = timeToRun;
	}

	/**
	 * 
	 */
	public void startTime() {
		this.startTime = System.currentTimeMillis();
	}
	
	/**
	 * 
	 */
	public void endTime() {
		this.endTime = System.currentTimeMillis();
		this.timeToRun = this.endTime - this.startTime;
	}
	
	/**
	 * To help w/ debugging
	 */
	public String toString() {
		return this.id + "__" + this.pixelString;
	}
	
	/**
	 * Create a deep copy of the pixel object
	 * @return
	 */
	public Pixel copy() {
		PixelAdapter adapter = new PixelAdapter();
		try {
			return adapter.fromJson(adapter.toJson(this));
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		return null;
	}
	
	/**
	 * Method to use to merge the translation pixel
	 * into the pixel for the recipe
	 * @param pixelObj
	 * @param mergePixel
	 */
	public static void translationMerge(Pixel pixelObj, Pixel mergePixel) {
		if(mergePixel != null) {
			pixelObj.setRefreshPanel(mergePixel.isRefreshPanel());
			pixelObj.setCodeDetails(mergePixel.isCodeExecution(), mergePixel.getCodeExecuted(), mergePixel.getLanguage(), mergePixel.isUserScript());
			pixelObj.setFrameTransformation(mergePixel.isFrameTransformation());
			pixelObj.setAssignment(mergePixel.isAssignment());
			pixelObj.setFileRead(mergePixel.isFileRead());
			pixelObj.setSaveDataTransformation(mergePixel.isSaveDataTransformation());
			pixelObj.setSaveDataExport(mergePixel.isSaveDataExport());
			pixelObj.setSaveVisualization(mergePixel.isSaveVisualization());
			pixelObj.setStartingFrameHeaders(mergePixel.getStartingFrameHeaders());
//			pixelObj.setReactorInputs(mergePixel.getReactorInputs());
			pixelObj.setFrameInputs(mergePixel.getFrameInputs());
			pixelObj.setFrameOutputs(mergePixel.getFrameOutputs());
			pixelObj.setTaskOptions(mergePixel.getTaskOptions());
			pixelObj.setRemoveLayerList(mergePixel.getRemoveLayerList());
			pixelObj.setCloneMapList(mergePixel.getCloneMapList());
			pixelObj.setTimeToRun(mergePixel.getTimeToRun());
		}
	}
	
	/**
	 * Parse the ending/starting frame headers map and get a map of alias to datatype
	 * @param headers
	 * @param frameName
	 * @return
	 */
	public static Map<String, String> getFrameHeadersToDataType(Map<String, Map<String, Object>> headersObject, String frameName) {
		Map<String, String> aliasToType = new HashMap<>();
		
		Map<String, Object> frameHeaders = headersObject.get(frameName);
		Map<String, Object> headerInfo = (Map<String, Object>) frameHeaders.get("headerInfo");
		List<Map<String, Object>> headers = (List<Map<String, Object>>) headerInfo.get("headers");
		for(Map<String, Object> headerMap : headers) {
			String alias = (String) headerMap.get("alias");
			String dataType = (String) headerMap.get("dataType");
			aliasToType.put(alias, dataType);
		}
		
		return aliasToType;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj == this) {
			return true;
		}
		if(obj instanceof Pixel) {
			Pixel otherP = (Pixel) obj;
			if(otherP.id.equals(this.id)
					&& otherP.pixelString.equals(this.pixelString)) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.id == null) ? 0 : this.id.hashCode());
		result = prime * result + ((this.pixelString == null) ? 0 : this.pixelString.hashCode());
		return result;
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
