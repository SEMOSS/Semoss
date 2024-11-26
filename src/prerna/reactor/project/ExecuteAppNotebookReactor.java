package prerna.reactor.project;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.NotebookExecution;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class ExecuteAppNotebookReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ExecuteAppNotebookReactor.class);

	/*
	 * This class is used to construct a new project
	 * This project only contains insights
	 */

	public ExecuteAppNotebookReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.MAP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			// you don't have access
			throw new IllegalArgumentException("Project/App does not exist or user does not have access to the project");
		}
		// grab user inputs
		Map<String, String> inputValueMap = new HashMap<>();
		Map<String, String> inputMap = getInputMap();
		for(String key : inputMap.keySet()) {
			inputValueMap.put(key, inputMap.get(key));
			inputValueMap.put(key+".value", inputMap.get(key));
		}
		
		Insight newInsight = new Insight();
		InsightUtility.transferDefaultVars(this.insight, newInsight);

		IProject project = Utility.getProject(projectId);
		NotebookExecution execution = project.executeNotebooks(newInsight, inputValueMap);
		
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", execution.getRunner());
		runnerWraper.put("variableOutput", execution.getVariableOutput());
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER);
		return noun;
	}
	
	private Map<String, String> getInputMap() {
		GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.MAP.getKey());
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, String>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, String>) mapInputs.get(0).getValue();
		}
		return null;
	}
}
