package prerna.sablecc;

import java.security.Permission;
import java.util.List;
import java.util.Vector;

import prerna.sablecc2.reactor.frame.py.PyReactor;
import prerna.sablecc2.reactor.frame.r.RReactor;
import prerna.sablecc2.reactor.runtime.AbstractBaseRClass;
import prerna.sablecc2.reactor.runtime.JavaReactor;

public class ReactorSecurityManager extends SecurityManager {

	private static List<String> classNamesToIgnore = new Vector<String>();
	static {
		classNamesToIgnore.add(JavaReactor.class.getName());
		classNamesToIgnore.add(AbstractBaseRClass.class.getName());
		classNamesToIgnore.add(RReactor.class.getName());
		classNamesToIgnore.add(PyReactor.class.getName());
	}
	
	public ReactorSecurityManager() {
		
	}
	
	public void addClass(String className) {
		classNamesToIgnore.add(className);
	}

	public void removeClass(String className) {
		classNamesToIgnore.remove(className);
	}
	
	public void checkPermission( Permission permission ) {
		if( permission.getName().contains("exitVM") ) {
			throw new SecurityException("Exit not permitted");
		}
	}
	
	// remove exit
	public void checkExit(int status) {
		if(blockThread()) {
			throw new SecurityException("Exit not permitted");
		}
	}

	// remove exec
	public void checkExec(String cmd) {
		if(blockThread()) {
			throw new SecurityException("Exec not permitted " + cmd);
		}
	}

	private boolean blockThread() {
		for (StackTraceElement elem : Thread.currentThread().getStackTrace()) {
			if (classNamesToIgnore.contains(elem.getClassName()) ) {
				return true;
			}
		}
		return false;
	}

}
