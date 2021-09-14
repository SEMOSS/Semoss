package prerna.sablecc2.reactor.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.logging.log4j.Logger;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewMethod;
import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.ReactorSecurityManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public final class JavaReactor extends AbstractReactor {

	private static final String CLASS_NAME = JavaReactor.class.getName();
	private static final String STACKTRACE = "StackTrace: ";

	// get the default manager
	private static transient SecurityManager defaultManager = System.getSecurityManager();
	
	@Override
	public NounMetadata execute() {
		
		String disable_terminal =  DIHelper.getInstance().getProperty(Constants.DISABLE_TERMINAL);
		if(disable_terminal != null && !disable_terminal.isEmpty() ) {
			 if(Boolean.parseBoolean(disable_terminal)) {
					throw new IllegalArgumentException("Terminal and user code execution has been disabled.");
			 };
		}
		
		ReactorSecurityManager tempManager = new ReactorSecurityManager();
		String className = "c" + System.currentTimeMillis();
		String packageName = "t" + System.currentTimeMillis();
		String uniqueName = packageName + "." + className;
		tempManager.addClass(uniqueName);

		Logger logger = getLogger(CLASS_NAME);
		this.store.toString();
		String code = Utility.decodeURIComponent(this.curRow.get(0).toString());
		try {
			ClassPool pool = ClassPool.getDefault();
			ClassClassPath ccp = new ClassClassPath(this.getClass());
			pool.insertClassPath(ccp); 
			pool.insertClassPath(new ClassClassPath(prerna.util.Console.class)); 

			//pool.importPackage("java.util");
			pool.importPackage("java.sql");
			pool.importPackage("prerna.sablecc2.om");
			pool.importPackage("prerna.util");
			pool.importPackage("org.apache.tinkerpop.gremlin.process.traversal");
			pool.importPackage("org.apache.tinkerpop.gremlin.structure");
			// the imports are sitting in the front
			while(code.contains("import ")) {
				String importStr = code.substring(code.indexOf("import"), code.indexOf(";") + 1);
				// remove this from data
				code = code.replace(importStr, "");
				importStr = importStr.replace(";", "");
				importStr = importStr.replace("import ", "");
				importStr = importStr.replace(".", "$");
				StringTokenizer importTokens = new StringTokenizer(importStr, "$");
				String packageStr = "";
				int tokenCount = importTokens.countTokens();
				for(int tokenIndex = 1; tokenIndex < tokenCount; tokenIndex++) {
					packageStr = packageStr + importTokens.nextToken();
					if(tokenIndex + 1 < tokenCount) {
						packageStr = packageStr + ".";
					}
				}
				logger.info("Importing.. [" + packageStr + "]");
				pool.importPackage(packageStr);
			}		

			CtClass cc = pool.makeClass(uniqueName); // the only reason I do this is if the user wants to do something else
			
			// the configuration of JRI vs. RServe
			// is now encapsulated within Abstract + RJavaTranslator
			cc.setSuperclass(pool.get(AbstractBaseRClass.class.getName()));

			String content = code;
			if(content.contains("runR")) {
				content = content.replace("\n", "\\n").replace("\r", "\\r");
			}

			// we only need to create the doMethod method
			// this will be wrapped in a try catch within the process
			// since for performance reasons, we want to make this method as small
			// as possible for faster compilation

			long start = System.currentTimeMillis();
			cc.addMethod(CtNewMethod.make("public void runCompiledCode() {" + content + ";}", cc));
			long end = System.currentTimeMillis();
			logger.debug(">>> Time to compile and add new class ::: " + (end-start) + " ms");
			Class retClass = cc.toClass();

			// next step is calling it
			AbstractBaseRClass jR = (AbstractBaseRClass) retClass.newInstance();
			jR.setInsight(this.insight);
			jR.setDataFrame((ITableDataFrame) this.insight.getDataMaker());
			jR.setPixelPlanner(this.planner);
			jR.setLogger(logger);
			jR.setRJavaTranslator(this.insight.getRJavaTranslator(this.getLogger(jR.getClass().getName())));
			// pass the managers inside
			jR.setCurSecurityManager(defaultManager);
			jR.setReactorManager(tempManager);
			jR.In();
			
			// set the security so we cant send some crazy virus into semoss
			System.setSecurityManager(tempManager);
			// call the process
			jR.runCompiledCode();

			// remove class from pool
			pool.removeClassPath(ccp);
			List<NounMetadata> outputs = jR.getNounMetaOutput();
			if(outputs == null || outputs.isEmpty()) {
				outputs = new ArrayList<>();
				if(jR.System.out.output != null) {
					outputs.add(new NounMetadata(jR.System.out.output.toString(), PixelDataType.CONST_STRING));
				} else if(jR.System.err.output != null) {
					outputs.add(new NounMetadata(jR.System.err.output.toString(), PixelDataType.CONST_STRING));
				}
			}
			return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
		} catch (RuntimeException re) {
			logger.error(STACKTRACE, re);
			throw re;
		} catch (CannotCompileException cce) {
			logger.error(STACKTRACE, cce);
			throw new IllegalArgumentException("Code had syntax errors which could not be compiled for execution: " + cce.getMessage());
		} catch (Exception ex) {
			logger.error(STACKTRACE, ex);
		} finally {
			tempManager.removeClass(uniqueName);
			// set back the original security manager
			System.setSecurityManager(defaultManager);	
		}
		
		return new NounMetadata("no output", PixelDataType.CONST_STRING, PixelOperationType.CODE_EXECUTION);
	}

}
