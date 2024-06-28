package prerna.project.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;
import prerna.util.Utility;

public class ProjectPropertyEvaluator {

	private static final Logger classLogger = LogManager.getLogger(ProjectPropertyEvaluator.class);
	private String projectId;
	private String methodName;
	private Object[] params;

	public ProjectPropertyEvaluator() {

	}

	public Object eval() {
		ProjectProperties props = Utility.getProject(this.projectId).getProjectProperties();
		try {
			Class[] paramTypes = null;
			if (this.params != null) {
				paramTypes = new Class[this.params.length];
				for (int i = 0; i < this.params.length; i++) {
					if (params[i] != null) {
						paramTypes[i] = this.params[i].getClass();
					}
				}
			}
			Method method = props.getClass().getMethod(this.methodName, paramTypes);
			Object ret = method.invoke(props, params);
			return ret;
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return null;
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public String getMethodName() {
		return methodName;
	}

	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}

}
