/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.logging.log4j.Logger;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewMethod;
import prerna.algorithm.api.ICodeExecution;
import prerna.om.Variable.LANGUAGE;
import prerna.reactor.AbstractReactor;
//import prerna.sablecc2.ReactorSecurityManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

@Deprecated
public final class JavaReactor extends AbstractReactor implements ICodeExecution {

	private static final String CLASS_NAME = JavaReactor.class.getName();
	// get the default manager
	// private static transient SecurityManager defaultManager =
	// System.getSecurityManager();
	// the code that was executed
	private String code = null;

	@Deprecated
	@Override
	public NounMetadata execute() {
		String retString = "Java console is now discontinued. Please use mvn directly for performing all java operations";
		retString = retString
				+ "Use mvn exec:java -Dexec.mainClass=\"your.company.className\" -Dexec.classpathScope=\"compile(if you want compile scope)\"";
		retString = retString + "Or just use java your.company.className";

		return new NounMetadata(retString, PixelDataType.CONST_STRING);
	}

	// @Override
	@Deprecated
	public NounMetadata execute_old() {
		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.DISABLE_TERMINAL))) {
			throw new IllegalArgumentException("Terminal and user code execution has been disabled.");
		}

		// check if java terminal is disabled
		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.DISABLE_JAVA_TERMINAL))) {
			throw new IllegalArgumentException("Java terminal has been disabled.");
		}

		String className = "c" + System.currentTimeMillis();
		String packageName = "t" + System.currentTimeMillis();
		String uniqueName = packageName + "." + className;

		Logger logger = getLogger(CLASS_NAME);
		this.store.toString();
		this.code = this.curRow.get(0).toString();
		try {
			ClassPool pool = ClassPool.getDefault();
			ClassClassPath ccp = new ClassClassPath(this.getClass());
			pool.insertClassPath(ccp);
			pool.insertClassPath(new ClassClassPath(prerna.util.Console.class));

			// pool.importPackage("java.util");
			pool.importPackage("java.sql");
			pool.importPackage("prerna.sablecc2.om");
			pool.importPackage("prerna.util");
			pool.importPackage("org.apache.tinkerpop.gremlin.process.traversal");
			pool.importPackage("org.apache.tinkerpop.gremlin.structure");
			// the imports are sitting in the front
			while (code.contains("import ")) {
				String importStr = code.substring(code.indexOf("import"), code.indexOf(";") + 1);
				// remove this from data
				code = code.replace(importStr, "");
				importStr = importStr.replace(";", "");
				importStr = importStr.replace("import ", "");
				importStr = importStr.replace(".", "$");
				StringTokenizer importTokens = new StringTokenizer(importStr, "$");
				String packageStr = "";
				int tokenCount = importTokens.countTokens();
				for (int tokenIndex = 1; tokenIndex < tokenCount; tokenIndex++) {
					packageStr = packageStr + importTokens.nextToken();
					if (tokenIndex + 1 < tokenCount) {
						packageStr = packageStr + ".";
					}
				}
				logger.info("Importing.. [" + packageStr + "]");
				pool.importPackage(packageStr);
			}

			// the only reason I do this is if the user wants to do something else
			CtClass cc = pool.makeClass(uniqueName);

			// the configuration of JRI vs. RServe
			// is now encapsulated within Abstract + RJavaTranslator
			cc.setSuperclass(pool.get(AbstractBaseRClass.class.getName()));

			String content = code;
			if (content.contains("runR")) {
				content = content.replace("\n", "\\n").replace("\r", "\\r");
			}

			// we only need to create the doMethod method
			// this will be wrapped in a try catch within the process
			// since for performance reasons, we want to make this method as small
			// as possible for faster compilation

			long start = System.currentTimeMillis();
			cc.addMethod(CtNewMethod.make("public void runCompiledCode() {" + content + ";}", cc));
			long end = System.currentTimeMillis();
			logger.debug(">>> Time to compile and add new class ::: " + (end - start) + " ms");
			Class<?> retClass = cc.toClass();

			// next step is calling it
			AbstractBaseRClass jR = (AbstractBaseRClass) retClass.newInstance();
			jR.setInsight(this.insight);
			jR.setDataFrame(this.insight.getCurrentFrame());
			jR.setPixelPlanner(this.planner);
			jR.setLogger(logger);
			jR.setRJavaTranslator(this.insight.getRJavaTranslator(this.getLogger(jR.getClass().getName())));
			jR.In();

			// call the process
			jR.runCompiledCode();

			// remove class from pool
			pool.removeClassPath(ccp);
			List<NounMetadata> outputs = jR.getNounMetaOutput();
			if (outputs == null || outputs.isEmpty()) {
				outputs = new ArrayList<>();
				if (jR.System.out.output != null) {
					outputs.add(new NounMetadata(jR.System.out.output.toString(), PixelDataType.CONST_STRING));
				} else if (jR.System.err.output != null) {
					outputs.add(new NounMetadata(jR.System.err.output.toString(), PixelDataType.CONST_STRING));
				}
			}
			return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
		} catch (RuntimeException re) {
			logger.error("Failed to run the java code", re);
			throw re;
		} catch (CannotCompileException cce) {
			logger.error("Failed to compile the java code", cce);
			throw new IllegalArgumentException(
					"Code had syntax errors which could not be compiled for execution: " + cce.getMessage());
		} catch (Exception ex) {
			logger.error("Failed to run the java code", ex);
		}

		return new NounMetadata("no output", PixelDataType.CONST_STRING, PixelOperationType.CODE_EXECUTION);
	}

	@Deprecated
	@Override
	public String getExecutedCode() {
		return this.code;
	}

	@Deprecated
	@Override
	public LANGUAGE getLanguage() {
		return LANGUAGE.JAVA;
	}

	@Deprecated
	@Override
	public boolean isUserScript() {
		return true;
	}
}
