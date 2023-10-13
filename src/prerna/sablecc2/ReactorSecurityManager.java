package prerna.sablecc2;

import java.security.Permission;
import java.util.List;
import java.util.Vector;

import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.ds.py.PyExecutorThread;
import prerna.engine.impl.r.RSingleton;
import prerna.reactor.frame.py.PyReactor;
import prerna.reactor.frame.r.RReactor;
import prerna.reactor.runtime.AbstractBaseRClass;
import prerna.reactor.runtime.JavaReactor;

public final class ReactorSecurityManager extends SecurityManager {

	private static List<String> classNamesToIgnore = new Vector<>();
	static {
		classNamesToIgnore.add(JavaReactor.class.getName());
		classNamesToIgnore.add(AbstractBaseRClass.class.getName());
		classNamesToIgnore.add(RReactor.class.getName());
		classNamesToIgnore.add(PyReactor.class.getName());
		classNamesToIgnore.add(PyExecutorThread.class.getName());
	}

	public ReactorSecurityManager() {
		// empty constructor
	}

	public void addClass(String className) {
		classNamesToIgnore.add(className);
	}

	public void removeClass(String className) {
		classNamesToIgnore.remove(className);
	}

	@Override
	public void checkPermission( Permission permission ) {
		if( permission.getName().contains("exitVM") ) {
			throw new SecurityException("Exit not permitted");
		}
	}
	
	// remove exit
	@Override
	public void checkExit(int status) {
		if(blockThread()) {
			throw new SecurityException("Exit not permitted");
		}
	}

	// remove exec
	@Override
	public void checkExec(String cmd) {
		if(blockThread()) {
			throw new SecurityException("Exec not permitted " + cmd);
		}
	}

	private boolean blockThread() {
		for (StackTraceElement elem : Thread.currentThread().getStackTrace()) {
			if(elem.getClassName().equals(RSingleton.class.getName())) {
				return false;
			}
			if(elem.getClassName().equals(WorkspaceAssetUtils.class.getName())) {
				return false;
			}
			if (classNamesToIgnore.contains(elem.getClassName()) ) {
				return true;
			}
		}
		return false;
	}

}
