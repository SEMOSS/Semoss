package prerna.engine.impl.vector.interceptor;

import java.lang.reflect.Constructor;

import net.sf.cglib.proxy.MethodInterceptor;
import prerna.engine.api.IVectorDatabaseEngine;

public abstract class AbstractInterceptor implements MethodInterceptor {
	
	protected final IVectorDatabaseEngine proxyEngine;
	protected final IVectorDatabaseEngine targetEngine;
	protected final Object[] constructorArgs;
	
	public AbstractInterceptor(IVectorDatabaseEngine proxyEngine, IVectorDatabaseEngine targetEngine, Object[] constructorArgs) {
		if(targetEngine == null) {
			throw new IllegalArgumentException("The vector database target for the proxy is undefined");
		}
		this.proxyEngine = proxyEngine;
		this.targetEngine = targetEngine;
		this.constructorArgs = constructorArgs;
	}
	
	@SuppressWarnings("unchecked")
	public static MethodInterceptor buildInterceptor(String interceptorClassName,IVectorDatabaseEngine proxy, IVectorDatabaseEngine target, Object[] constructorArgs) throws Exception {
		Class<? extends MethodInterceptor> interceptorClass;
		try {
			interceptorClass = (Class<? extends MethodInterceptor>) Class.forName(interceptorClassName);
		} catch(ClassNotFoundException | ClassCastException e) {
			e.printStackTrace();
			throw e;
		}
		return buildInterceptor(interceptorClass, proxy, target, constructorArgs);
	}
	
	@SuppressWarnings("unchecked")
	public static <T extends MethodInterceptor> T buildInterceptor(Class<T> interceptorClass,IVectorDatabaseEngine proxy, IVectorDatabaseEngine target, Object[] constructorArgs) throws Exception {
		Constructor<?> constructor;
		try {
			constructor = interceptorClass.getConstructor(IVectorDatabaseEngine.class, IVectorDatabaseEngine.class, Object[].class);
			return (T) constructor.newInstance(proxy, target, constructorArgs);
		} catch(NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
			throw e;
		}
	}
}
