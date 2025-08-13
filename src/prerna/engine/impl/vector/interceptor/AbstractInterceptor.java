package prerna.engine.impl.vector.interceptor;

import java.lang.reflect.Constructor;

import net.sf.cglib.proxy.MethodInterceptor;
import prerna.engine.api.IVectorDatabaseEngine;

public abstract class AbstractInterceptor implements MethodInterceptor {
	
	protected final IVectorDatabaseEngine target;
	protected final Object[] constructorArgs;
	
	public AbstractInterceptor(IVectorDatabaseEngine target, Object[] constructorArgs) {
		if(target == null) {
			throw new IllegalArgumentException("The vector database target for the proxy is undefined");
		}
		this.target = target;
		this.constructorArgs = constructorArgs;
	}
	
	@SuppressWarnings("unchecked")
	public static MethodInterceptor buildInterceptor(String interceptorClassName, IVectorDatabaseEngine target, Object[] constructorArgs) throws Exception {
		Class<? extends MethodInterceptor> interceptorClass;
		try {
			interceptorClass = (Class<? extends MethodInterceptor>) Class.forName(interceptorClassName);
		} catch(ClassNotFoundException | ClassCastException e) {
			e.printStackTrace();
			throw e;
		}
		return buildInterceptor(interceptorClass, target, constructorArgs);
	}
	
	@SuppressWarnings("unchecked")
	public static <T extends MethodInterceptor> T buildInterceptor(Class<T> interceptorClass, IVectorDatabaseEngine target, Object[] constructorArgs) throws Exception {
		Constructor<?> constructor;
		try {
			constructor = interceptorClass.getConstructor(IVectorDatabaseEngine.class, Object[].class);
			return (T) constructor.newInstance(target, constructorArgs);
		} catch(NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
			throw e;
		}
	}
}
