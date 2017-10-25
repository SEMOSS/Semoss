package prerna.sablecc2.reactor.runtime;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.ReactorSecurityManager;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class JavaReactor extends AbstractReactor {

	private static final String CLASS_NAME = JavaReactor.class.getName();
	private transient SecurityManager curManager = null;
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		this.store.toString();
		String code = this.curRow.get(0).toString();
		code = code.substring(3, code.length()-4).trim();
		try {
			code = URLDecoder.decode(code, "UTF-8").replaceAll("\\%20", "+");
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}
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
				System.out.println("Importing.. [" + packageStr + "]");
				pool.importPackage(packageStr);
			}		

			String packageName = "t" + System.currentTimeMillis(); // make it unique
			CtClass cc = pool.makeClass(packageName + ".c" + System.currentTimeMillis()); // the only reason I do this is if the user wants to do something else
			
			// need to find what is the R configuration 
			// and then wrap specific class
			String useJriStr = DIHelper.getInstance().getProperty(Constants.R_CONNECTION_JRI);
			boolean useJri = false;
			if(useJriStr != null) {
				useJri = Boolean.valueOf(useJriStr);
			}
			if(useJri) {
				cc.setSuperclass(pool.get("prerna.sablecc2.reactor.runtime.AbstractBaseJriRImpl"));
			} else {
				cc.setSuperclass(pool.get("prerna.sablecc2.reactor.runtime.AbstractBaseRserveRImpl"));
			}

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
			jR.In();
			
			// get the original security
			this.curManager =  System.getSecurityManager();
			// set the security so we cant send some crazy virus into semoss
			System.setSecurityManager(new ReactorSecurityManager());
			// call the process
			jR.runCompiledCode();
			// set back the original security manager
			System.setSecurityManager(this.curManager);			

			// remove class from pool
			pool.removeClassPath(ccp);
			List<NounMetadata> outputs = jR.getNounMetaOutput();
			if(outputs == null || outputs.isEmpty()) {
				outputs = new ArrayList<NounMetadata>();
				if(jR.System.out.output != null) {
					outputs.add(new NounMetadata(jR.System.out.output.toString(), PixelDataType.CONST_STRING));
				} else if(jR.System.err.output != null) {
					outputs.add(new NounMetadata(jR.System.err.output.toString(), PixelDataType.CONST_STRING));
				}
			}
			return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (CannotCompileException e) {
			e.printStackTrace();
		} catch (NotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		
		return new NounMetadata("no output", PixelDataType.CONST_STRING, PixelOperationType.CODE_EXECUTION);
	}

}
