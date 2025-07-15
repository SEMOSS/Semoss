package prerna.engine.impl.pipeline;

import java.lang.reflect.Proxy;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.project.api.IProject;

/**
 * Factory for creating a dynamic proxy that wraps an IEngine instance
 * to apply input and output processing pipelines.
 */
public class EngineProxyFactory {

    /**
     * Creates a guarded IEngine instance that intercepts method calls.
     *
     * @param realEngine The actual engine instance to wrap.
     * @param pipelineJson The JSON string defining the pipelines.
     * @return A proxy that implements IEngine and applies the defined pipelines.
     */
	// we probably dont need this anymore.. 
    public static IEngine createGuardedEngine(IEngine realEngine) {
    	
        PipelineInvocationHandler handler = new PipelineInvocationHandler(realEngine);
        
        return (IEngine) Proxy.newProxyInstance(
                IEngine.class.getClassLoader(),
                new Class<?>[] { IEngine.class, IModelEngine.class},
                handler
        );
    }
    
    public static IModelEngine createGuardedModelEngine(IModelEngine realEngine) {

		if(realEngine != null && realEngine.getSmssProp().containsKey(IEngine.PIPELINE))
		{
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(realEngine);
	        return (IModelEngine) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IModelEngine.class},
	                handler
	        );
		}
		else
			return realEngine;
    }

    public static IDatabaseEngine createGuardedDatabaseEngine(IDatabaseEngine realEngine) {

		if(realEngine != null && realEngine.getSmssProp().containsKey(IEngine.PIPELINE))
		{
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(realEngine);
	        return (IDatabaseEngine) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IDatabaseEngine.class},
	                handler
	        );
		}
		else
			return realEngine;

        
    }


    public static IStorageEngine createGuardedStorageEngine(IStorageEngine realEngine) {
		if(realEngine != null && realEngine.getSmssProp().containsKey(IEngine.PIPELINE))
		{
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(realEngine);
	        return (IStorageEngine) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IStorageEngine.class},
	                handler
	        );
		}
		else
			return realEngine;
    }

    
    public static IFunctionEngine createGuardedFunctionEngine(IFunctionEngine realEngine) {
		if(realEngine != null && realEngine.getSmssProp().containsKey(IEngine.PIPELINE))
		{
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(realEngine);
	        return (IFunctionEngine) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IFunctionEngine.class},
	                handler
	        );
		}
		else
			return realEngine;
    }

    
    
    public static IVectorDatabaseEngine createGuardedVectorEngine(IVectorDatabaseEngine realEngine) {
		if(realEngine != null && realEngine.getSmssProp().containsKey(IEngine.PIPELINE))
		{
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(realEngine);
	        return (IVectorDatabaseEngine) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IVectorDatabaseEngine.class},
	                handler
	        );
		}
		else
			return realEngine;
    }


    public static IProject createGuardedProject(IProject realEngine) {
		if(realEngine != null && realEngine.getSmssProp().containsKey(IEngine.PIPELINE))
		{
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(realEngine);
	        return (IProject) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IProject.class},
	                handler
	        );
		}
		else
			return realEngine;
    }

    
    
    
    
    
}