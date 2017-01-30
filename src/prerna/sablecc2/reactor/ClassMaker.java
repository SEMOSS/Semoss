package prerna.sablecc2.reactor;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewMethod;
import javassist.NotFoundException;

public class ClassMaker {

	ClassPool pool = null;
	CtClass cc = null;
	boolean hasSuper = false;
	
	public ClassMaker(String packageName)
	{
		pool = ClassPool.getDefault();
		pool.insertClassPath(new ClassClassPath(this.getClass())); 
		pool.insertClassPath(new ClassClassPath(prerna.util.Console.class)); 
		//pool.importPackage("java.util");
		pool.importPackage("java.sql");
		pool.importPackage("java.lang");
		pool.importPackage("java.util");
		pool.importPackage("prerna.util");
		pool.importPackage("org.apache.tinkerpop.gremlin.process.traversal");
		pool.importPackage("org.apache.tinkerpop.gremlin.structure");
		pool.importPackage("prerna.ds");

		//packageName = "t" + System.currentTimeMillis(); // make it unique
		//CtClass consoleClass = pool.get("prerna.util.Console");
		
		cc = pool.makeClass(packageName + ".c" + System.currentTimeMillis()); // the only reason I do this is if the user wants to do seomthing else
	}
	
	public ClassMaker()
	{
		this("t" + System.currentTimeMillis());
	}
	
	//sets the interface
	public void addInterface(String interfaceName)
	{
		try {
			CtClass[] interfaceVector = new CtClass[1];
			interfaceVector[0] = pool.getCtClass(interfaceName);
			cc.setInterfaces(interfaceVector);
		} catch (NotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// add a super class
	public void addSuper(String superClassName)
	{
		if(!hasSuper)
		{
			try {
				cc.setSuperclass(pool.getCtClass(superClassName));
			} catch (NotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (CannotCompileException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			hasSuper = true;
		}
	}
	
	// add a method
	public void addMethod(String daMethod)
	{
		try {
			cc.addMethod(CtNewMethod.make(daMethod, cc));
		} catch (CannotCompileException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// gets the class back
	public Class toClass()
	{
		try {
			return cc.toClass();
		} catch (CannotCompileException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
	
	public void writeClass(String fileName)
	{
        try {
    		DataOutputStream out = new DataOutputStream(new FileOutputStream(fileName));
			cc.getClassFile().write(out);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
