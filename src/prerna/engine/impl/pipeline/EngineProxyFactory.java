package prerna.engine.impl.pipeline;

import java.lang.reflect.Proxy;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IReactorFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.IVenvEngine;
import prerna.project.api.IProject;

/**
 * Factory for creating a dynamic proxy that wraps an IEngine instance
 * to apply input and output processing pipelines.
 */
public class EngineProxyFactory {

    /**
     * 
     * @param engine
     * @return
     */
    public static IModelEngine createGuardedModelEngine(IModelEngine engine) {
		if(engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(engine);
	        return (IModelEngine) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IModelEngine.class},
	                handler
	        );
		}
		
		return engine;
    }

    /**
     * 
     * @param engine
     * @return
     */
    public static IDatabaseEngine createGuardedDatabaseEngine(IDatabaseEngine engine) {
		if(engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(engine);
	        return (IDatabaseEngine) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IDatabaseEngine.class},
	                handler
	        );
		}
		return engine;
    }

    /**
     * 
     * @param engine
     * @return
     */
    public static IStorageEngine createGuardedStorageEngine(IStorageEngine engine) {
		if(engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(engine);
	        return (IStorageEngine) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IStorageEngine.class},
	                handler
	        );
		}
		return engine;
    }

    /**
     * 
     * @param engine
     * @return
     */
    public static IFunctionEngine createGuardedFunctionEngine(IFunctionEngine engine) {
		if(engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(engine);
	        return (IFunctionEngine) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IFunctionEngine.class},
	                handler
	        );
		}
		return engine;
    }
    
    /**
     * 
     * @param engine
     * @return
     */
	public static IReactorFunctionEngine createGuardedReactorEngine(IReactorFunctionEngine engine) {
		if(engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(engine);
	        return (IReactorFunctionEngine) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IReactorFunctionEngine.class},
	                handler
	        );
		}
		return engine;
    }

    /**
     * 
     * @param engine
     * @return
     */
    public static IVectorDatabaseEngine createGuardedVectorEngine(IVectorDatabaseEngine engine) {
		if(engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(engine);
	        return (IVectorDatabaseEngine) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IVectorDatabaseEngine.class},
	                handler
	        );
		}
		
		return engine;
    }

    /**
     * 
     * @param engine
     * @return
     */
    public static IProject createGuardedProject(IProject engine) {
		if(engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(engine);
	        return (IProject) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IProject.class},
	                handler
	        );
		}
		
		return engine;
    }

    /**
     * 
     * @param engine
     * @return
     */
	public static IVenvEngine createGuardedVenvEngine(IVenvEngine engine) {
		if(engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
	        PipelineInvocationHandler handler = new PipelineInvocationHandler(engine);
	        return (IVenvEngine) Proxy.newProxyInstance(
	                IEngine.class.getClassLoader(),
	                new Class<?>[] { IEngine.class, IVenvEngine.class},
	                handler
	        );
		}
		
		return engine;
    }



}