package prerna.reactor.project;

import java.util.ArrayList;
import java.util.List;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractProjectPyReactor extends AbstractReactor {

	public AbstractProjectPyReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CODE.getKey(), ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		String disable_terminal = DIHelper.getInstance().getProperty(Constants.DISABLE_TERMINAL);
		if (disable_terminal != null && !disable_terminal.isEmpty()) {
			if (Boolean.parseBoolean(disable_terminal)) {
				throw new IllegalArgumentException("Terminal and user code execution has been disabled.");
			}
		}

		// check if py terminal is disabled
		String disable_py_terminal = DIHelper.getInstance().getProperty(Constants.DISABLE_PY_TERMINAL);
		if (disable_py_terminal != null && !disable_py_terminal.isEmpty()) {
			if (Boolean.parseBoolean(disable_py_terminal)) {
				throw new IllegalArgumentException("Python terminal has been disabled.");
			}
		}

		if (!PyUtils.pyEnabled()) {
			throw new IllegalArgumentException("Python is not enabled to use the following command");
		}

		organizeKeys();
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			projectId = this.insight.getContextProjectId();
			if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
				projectId = this.insight.getProjectId();
			}
		}
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}

		String code = getDecodedCode();

		// make sure valid id for user
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			// you don't have access
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		IProject project = Utility.getProject(projectId);
		PyTranslator projectPyTranslator = project.getProjectPyTranslator();
		Object output = projectPyTranslator.runScript(code);

		List<NounMetadata> outputs = new ArrayList<>(1);
		outputs.add(new NounMetadata(output + "", PixelDataType.CONST_STRING));
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}

	@Override
	public String getReactorDescription() {
		return "Run Python code in the project's dedicated python process";
	}

	/**
	 * Decode the code string
	 * 
	 * @return The decoded code string
	 */
	protected abstract String getDecodedCode();
}
