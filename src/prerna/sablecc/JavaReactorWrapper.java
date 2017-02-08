package prerna.sablecc;

import java.io.File;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IScriptReactor;
import prerna.sablecc.meta.ExecuteCodePkqlMetadata;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class JavaReactorWrapper extends AbstractReactor {
	
	private static final Logger LOGGER = LogManager.getLogger(JavaReactorWrapper.class.getName());
	
	SecurityManager securityManager = new ReactorSecurityManager();
	SecurityManager curManager = null;
	IScriptReactor daReactor = null;
	
	public JavaReactorWrapper()
	{
		String [] thisReacts = {};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.JAVA_OP;
	}
	
	@Override
	public Iterator process() {
		PKQLRunner thisRunner = (PKQLRunner)myStore.get("PKQLRunner");
		
		String useJriStr = DIHelper.getInstance().getProperty(Constants.R_CONNECTION_JRI);
		boolean useJri = false;
		if(useJriStr != null) {
			useJri = Boolean.valueOf(useJriStr);
		}
		ITableDataFrame dataFrame = (ITableDataFrame)myStore.get("G");
		// hold up, if frame is r data table
		// use that connection
		// which is always running on its own Rserve
		if(!useJri && dataFrame instanceof RDataTable) {
			LOGGER.info("Utilizing R connection details from RDataTable...");
			thisRunner.setVariableValue(AbstractRJavaReactor.R_CONN, ((RDataTable) dataFrame).getConnection() );
			thisRunner.setVariableValue(AbstractRJavaReactor.R_PORT, ((RDataTable) dataFrame).getPort() );
		}
		
		System.out.println(myStore);
		
		try {
			ClassPool pool = ClassPool.getDefault();
			pool.insertClassPath(new ClassClassPath(this.getClass())); 
			pool.insertClassPath(new ClassClassPath(prerna.util.Console.class)); 
			//pool.importPackage("java.util");
			pool.importPackage("java.sql");
			pool.importPackage("prerna.util");
			pool.importPackage("org.apache.tinkerpop.gremlin.process.traversal");
			pool.importPackage("org.apache.tinkerpop.gremlin.structure");
			

			String data = myStore.get(PKQLEnum.JAVA_OP) + "";
			data = data.replace("<code>", "");
			
			// the imports are sitting in the front
			while(data.contains("import "))
			{
				String importStr = data.substring(data.indexOf("import"), data.indexOf(";") + 1);
				// remove this from data
				data = data.replace(importStr, "");
				importStr = importStr.replace(";", "");
				importStr = importStr.replace("import ", "");
				importStr = importStr.replace(".", "$");
				StringTokenizer importTokens = new StringTokenizer(importStr, "$");
				String packageStr = "";
				int tokenCount = importTokens.countTokens();
				for(int tokenIndex = 1;tokenIndex < tokenCount;tokenIndex++)
				{
					packageStr = packageStr + importTokens.nextToken();
					if(tokenIndex + 1 < tokenCount)
						packageStr = packageStr + ".";
				}
				System.out.println("Importing.. [" + packageStr + "]");
				pool.importPackage(packageStr);
			}		
			
			String	packageName = "t" + System.currentTimeMillis(); // make it unique
			//CtClass consoleClass = pool.get("prerna.util.Console");
			
			CtClass cc = pool.makeClass(packageName + ".c" + System.currentTimeMillis()); // the only reason I do this is if the user wants to do seomthing else
			// need to find what is the configuration 
			// and then wrap specific class
			if(useJri) {
				cc.setSuperclass(pool.get("prerna.sablecc.BaseJavaReactorJRI"));
			} else {
				cc.setSuperclass(pool.get("prerna.sablecc.BaseJavaReactor"));
			}
			//cc.addField(new CtField(consoleClass, "System", cc));
			Class retClass = null;
			// this is what comes in from the front end
			//BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			//String data = reader.readLine();
			String tryStr = "try {";
			String catchStr = "}catch (Exception ex) { "
					+ "if( ex.getMessage() != null && !ex.getMessage().isEmpty() ) {"
						+ "put(\"RESPONSE\", \"ERROR : \" + ex.getMessage()); "
					+ "} else {"
						+ "put(\"RESPONSE\", \"Failed\"); "
					+ "}"
					+ "put(\"STATUS\" , prerna.sablecc.PKQLRunner.STATUS.ERROR); "
					+ "put(\"ERROR\", ex);return null;"
					+ "}";
			String content = data;
			if(content.contains("runR")) {
				content = content.replace("\n", "\\n").replace("\r", "\\r");
			}
			// write the response
			String response = "put(\"RESPONSE\", System.out.output); put(\"STATUS\" , prerna.sablecc.PKQLRunner.STATUS.SUCCESS); return null;";
			//cc.addMethod(CtNewMethod.make("public void setConsole() { System = new prerna.util.Console();}", cc));
			cc.addMethod(CtNewMethod.make("public java.util.Iterator process() {" +
											//"super.process();" + 
											tryStr + 
											content+ ";" + 
											response + 
											catchStr + "}", cc));
//			try {
//				cc.writeFile();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			retClass = cc.toClass();
			// next step is calling it
			AbstractJavaReactor jR = (AbstractJavaReactor)retClass.newInstance();
			curManager =  System.getSecurityManager();
		    //jR.setConsole();
		    // set the data frame first
		    jR.setDataFrame(dataFrame);
		    
		    // set the PKQL Runner as well
		    jR.setPKQLRunner(thisRunner);
		    
		    // call the process
		    System.setSecurityManager( new ReactorSecurityManager()) ;		    
			jR.process();
		    System.setSecurityManager( curManager) ;			
			
			// reset the frame
			if(jR.frameChanged) {
				jR.put("G", jR.dataframe);
				myStore.put("G", jR.dataframe);
			}
			if(jR.hasReturnData) {
				jR.put("returnData", jR.returnData);
				myStore.put("returnData", jR.returnData);
			}
			System.out.println(jR.getValue("ERROR"));
			daReactor = jR;
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
		return null;
	}

	@Override
	public String[] getParams() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void set(String key, Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addReplacer(String pattern, Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String[] getValues2Sync(String childName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeReplacer(String pattern) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getValue(String key) {
		return daReactor.getValue(key);
	}

	public IPkqlMetadata getPkqlMetadata() {
		ExecuteCodePkqlMetadata pkqlMetadata = new ExecuteCodePkqlMetadata();
		pkqlMetadata.setPkqlStr(myStore.get(PKQLEnum.JAVA_OP).toString());
		pkqlMetadata.setExecutorType("Java");
		pkqlMetadata.setExecutedCode(myStore.get(PKQLEnum.JAVA_OP).toString().replace("<code>", ""));
		return pkqlMetadata;
	}


	//-------------- Test Methods ------------------
	
	public void tryPKQL()
	{
		PKQLRunner runner = new PKQLRunner();
		runner.runPKQL("2 + 2;", new H2Frame());
	}

	public void tryExit()
	{
		
		   SecurityManager curManager =  System.getSecurityManager();
		    System.setSecurityManager( securityManager ) ;
		    try
		    {
		    	System.out.println("Here.. ");
			    //System.exit(0);
			    File file = new File("temp");
			    file.canRead();
		    	
		    }catch (SecurityException ex)
		    {
		    	System.out.println(ex.getMessage());
		    }
		    System.setSecurityManager( curManager) ;
	}

	public static void main(String [] args)
	{
		JavaReactorWrapper jr = new JavaReactorWrapper();
		
		jr.process();
		//jr.tryExit();
		//jr.tryPKQL();
	}
	
	

	
}
