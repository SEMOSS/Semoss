/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
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
