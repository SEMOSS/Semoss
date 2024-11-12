package prerna.reactor.project;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelRunner;
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
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			// you don't have access
			throw new IllegalArgumentException("Project/App does not exist or user does not have access to the project");
		}
		
		Insight newInsight = new Insight();
		InsightUtility.transferDefaultVars(this.insight, newInsight);

		IProject project = Utility.getProject(projectId);
		PixelRunner runner = project.executeNotebooks(newInsight);
		
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", runner);
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER);
		return noun;
	}
	
}
