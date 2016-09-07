package prerna.sablecc;

import java.io.File;
import java.util.Iterator;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.H2.H2Frame;
import prerna.engine.api.IScriptReactor;

public class JavaReactorWrapper extends AbstractReactor {
	
	SecurityManager securityManager = new ReactorSecurityManager();
	SecurityManager curManager = null;
	IScriptReactor daReactor = null;
	
	public JavaReactorWrapper()
	{
		String [] thisReacts = {};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.JAVA_OP;

		// setting pkqlMetaData
		String title = "Execute java code as a script";
		String pkqlCommand = "j:<code>java_code<code>;";
		String description = "Executes a piece of java code as a script";
		boolean showMenu = true; // need to understand this
		boolean pinned = true;
		super.setPKQLMetaData(title, pkqlCommand, description, showMenu, pinned);
		//super.setPKQLMetaDataInput(populatePKQLMetaDataInput());
		super.setPKQLMetaDataInput();

	}
	
	@Override
	public Iterator process() {

		System.out.println(myStore);
		
		try {
			ClassPool pool = ClassPool.getDefault();
			pool.insertClassPath(new ClassClassPath(this.getClass())); 
			String	packageName = "t" + System.currentTimeMillis(); // make it unique
			CtClass cc = pool.makeClass(packageName + ".c" + System.currentTimeMillis()); // the only reason I do this is if the user wants to do seomthing else
			cc.setSuperclass(pool.get("prerna.sablecc.BaseJavaReactor"));
			Class retClass = null;
			// this is what comes in from the front end
			//BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			//String data = reader.readLine();
			String data = myStore.get(PKQLEnum.JAVA_OP) + "";
			data = data.replace("<code>", "");
			String tryStr = "try {";
			String catchStr = "}catch (Exception ex) { put(\"RESPONSE\", \"Failed\"); put(\"STATUS\" , prerna.sablecc.PKQLRunner.STATUS.ERROR); put(\"ERROR\", ex);return null;}";
			String content = data;
			// write the response
			String response = "put(\"RESPONSE\", \"Complete\"); put(\"STATUS\" , prerna.sablecc.PKQLRunner.STATUS.SUCCESS); return null;";
			cc.addMethod(CtNewMethod.make("public java.util.Iterator process() {" + 
											tryStr + 
											content+ ";" + 
											response + 
											catchStr + "}", cc));
			//cc.writeFile();
			retClass = cc.toClass();
			// next step is calling it
			BaseJavaReactor jR = (BaseJavaReactor)retClass.newInstance();
			curManager =  System.getSecurityManager();
		    
		    // set the data frame first
		    jR.setDataFrame((ITableDataFrame)myStore.get("G"));
		    
		    // set the PKQL Runner as well
		    jR.setPKQLRunner((PKQLRunner)myStore.get("PKQLRunner"));
		    
		    // call the process
		    System.setSecurityManager( securityManager ) ;		    
			jR.process();
		    System.setSecurityManager( curManager) ;			
			
			// reset the frame
			if(jR.frameChanged)
				myStore.put("G", jR.dataframe);
			System.out.println(jR.getValue("ERROR"));
			daReactor = jR;
		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CannotCompileException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} /*catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} */catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
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
		// TODO Auto-generated method stub
		return daReactor.getValue(key);
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
