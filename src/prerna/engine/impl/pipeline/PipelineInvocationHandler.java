package prerna.engine.impl.pipeline;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.reactor.IReactor;
import prerna.reactor.interceptor.IInputReactor;
import prerna.reactor.interceptor.IOutputReactor;
import prerna.reactor.interceptor.PipelineReactorUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;

/**
 * The invocation handler for the dynamic proxy. This class intercepts all method calls,
 * executes the appropriate pipelines, and then calls the real engine method.
 */
public class PipelineInvocationHandler implements InvocationHandler {

    private static final Logger classLogger = LogManager.getLogger(PipelineInvocationHandler.class);

    private final IEngine realEngine;
    private final Map<String, Pipeline> pipelinesMap = new HashMap<>();

    /**
     * 
     * @param realEngine
     */
    public PipelineInvocationHandler(IEngine realEngine) {
        this.realEngine = realEngine;
        String pipelineJson = getJsonData(realEngine);
        parseAndLoadPipelines(pipelineJson);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        // Find the correct pipeline for the called method.
        Pipeline specificPipeline = this.pipelinesMap.get(methodName);
        if (specificPipeline == null) {
            specificPipeline = this.pipelinesMap.get("*");
        }

        if (specificPipeline == null) {
            // No pipeline defined for this method, so just invoke the real method.
            return method.invoke(this.realEngine, args);
        }

        Map<String, Object> processedArguments = null;
        int inputIndex = 0; 	
        // === INPUT PIPELINE EXECUTION ===
        try {
            for (IInputReactor reactor : specificPipeline.getInputPipeline()) {
                processedArguments = mapArguments(reactor, method, args);
                NounStore inputNouns = new NounStore("input-pipeline");
                
                GenRowStruct grs = new GenRowStruct();
                processedArguments.put(PipelineReactorUtils.ENGINE, realEngine);
                processedArguments.put(PipelineReactorUtils.METHOD_NAME, method);
                processedArguments.put(PipelineReactorUtils.CONFIG, specificPipeline.getInputParams().get(inputIndex));
                grs.add(new NounMetadata(processedArguments, PixelDataType.MAP));
                inputNouns.addNoun(PipelineReactorUtils.ARGUMENTS, grs);
                reactor.setNounStore(inputNouns);

                NounMetadata resultNoun = reactor.execute();
                processedArguments = (Map<String, Object>) resultNoun.getValue();
                inputIndex++;
                
                // get the decision and if it is false
                // stop the execution. 
                Map resultMap = (Map <String, Object>) processedArguments.get(PipelineReactorUtils.INTERIM_RESULT);
                boolean pass = (boolean)resultMap.get(PipelineReactorUtils.PASS);
                if(!pass)
                		throw new SecurityException("Input Guardrail issue detected");
                
            }
        } catch (SecurityException e) {
            classLogger.error("Input pipeline blocked execution for method " + methodName, e);
            throw e;
        }

        Object[] finalArgs = unmapArguments(method, processedArguments);

        // === ACTUAL METHOD EXECUTION ===
        Object result = method.invoke(this.realEngine, finalArgs);
        
        processedArguments.put(PipelineReactorUtils.RESULT, result);

        // === OUTPUT PIPELINE EXECUTION ===
        inputIndex = 0;
        for (IOutputReactor reactor : specificPipeline.getOutputPipeline()) {
            NounStore outputNouns = new NounStore("output-pipeline");
            GenRowStruct grs = new GenRowStruct();
            processedArguments.put(PipelineReactorUtils.ENGINE, realEngine);
            processedArguments.put(PipelineReactorUtils.METHOD_NAME, method);
            processedArguments.put(PipelineReactorUtils.CONFIG, specificPipeline.getOutputParams().get(inputIndex));
            grs.add(new NounMetadata(processedArguments, PixelDataType.MAP));
            outputNouns.addNoun(PipelineReactorUtils.ARGUMENTS, grs);            
            reactor.setNounStore(outputNouns);

            NounMetadata resultNoun = reactor.execute();
            processedArguments = (Map<String, Object>)resultNoun.getValue();
            inputIndex++;
            
            // eval result	
            Map resultMap = (Map <String, Object>) processedArguments.get(PipelineReactorUtils.INTERIM_RESULT);
            boolean pass = (boolean)resultMap.get(PipelineReactorUtils.PASS);
            if(!pass)
            		throw new SecurityException("Output Guardrail issue detected");

        }

        return processedArguments.get(PipelineReactorUtils.RESULT);
    }
    
    /**
     * 
     * @param engine
     * @return
     */
	private static String getJsonData(IEngine engine) {
		String jsonString = null;
		try {
			String versionFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(), engine.getEngineName());
			String pipelineFile = versionFolder + "/" + engine.getSmssProp().getProperty(IEngine.PIPELINE);
			pipelineFile = pipelineFile.replace("\\", "/");
			jsonString = FileUtils.readFileToString(new File(pipelineFile), "UTF-8");
		} catch (IOException e) {
        	classLogger.error(Constants.STACKTRACE, e);
		}
		return jsonString;
	}

	/**
	 * 
	 * @param pipelineJson
	 */
    private void parseAndLoadPipelines(String pipelineJson) {
        JSONObject root = new JSONObject(pipelineJson);
        JSONObject pipelines = root.getJSONObject("pipelines");

        for (String methodName : pipelines.keySet()) {
            JSONObject pipelineConfig = pipelines.getJSONObject(methodName);
            List<IInputReactor> inputReactors = new ArrayList<>();
            List<IOutputReactor> outputReactors = new ArrayList<>();
            List<Map<String, Object>> inputParams = new ArrayList<>();
            List<Map<String, Object>> outputParams = new ArrayList<>();

            if (pipelineConfig.has("input")) {
                JSONArray inputArray = pipelineConfig.getJSONArray("input");
                for (int i = 0; i < inputArray.length(); i++) {
                    JSONObject reactorConfig = inputArray.getJSONObject(i);
                    IInputReactor inputReactor = createReactor(reactorConfig, IInputReactor.class);
                    Map <String, Object> inputParam = (Map<String, Object>)inputReactor.getNounStore().getNoun("param").get(0);
                    inputReactors.add(inputReactor);
                    inputParams.add(inputParam);
                }
            }

            if (pipelineConfig.has("output")) {
                JSONArray outputArray = pipelineConfig.getJSONArray("output");
                for (int i = 0; i < outputArray.length(); i++) {
                    JSONObject reactorConfig = outputArray.getJSONObject(i);
                    IOutputReactor outputReactor = createReactor(reactorConfig, IOutputReactor.class);
                    Map <String, Object> outputParam = (Map<String, Object>)outputReactor.getNounStore().getNoun("param").get(0);
                    outputParams.add(outputParam);
                    outputReactors.add(outputReactor);
                }
            }

            this.pipelinesMap.put(methodName, new Pipeline(inputReactors, outputReactors, inputParams, outputParams));
        }
    }

    private <T extends IReactor> T createReactor(JSONObject config, Class<T> reactorType) {
        String className = config.getString("reactorClass");
        try {
            Class<?> clazz = Class.forName(className);
            T reactor = reactorType.cast(clazz.newInstance());
            GenRowStruct grs = new GenRowStruct();
            if (config.has("params")) {
                JSONObject params = config.getJSONObject("params");
                NounStore nounStore = new NounStore("Reactor-params");
                Map <String,Object> paramMap = new HashMap();
                for (String key : params.keySet()) {
                    Object value = params.get(key);
                    paramMap.put(key, value);
                }
                grs.add(new NounMetadata(paramMap, PixelDataType.MAP));
                nounStore.addNoun("param", grs);
                reactor.setNounStore(nounStore);
            }

            return reactor;
        } catch (Exception e) {
        	classLogger.error(Constants.STACKTRACE, e);
            throw new RuntimeException("Failed to create reactor: " + className, e);
        }
    }

    /**
     * 
     * @param reactor
     * @param method
     * @param args
     * @return
     */
    private Map<String, Object> mapArguments(IReactor reactor, Method method, Object[] args) {
        Map<String, Object> map = new HashMap<>();
        Parameter[] parameters = method.getParameters();
        for (int i = 0; i < parameters.length; i++) {
        		if(args[i] instanceof Insight)
        			reactor.setInsight((Insight)args[i]);
            map.put(parameters[i].getName(), args[i]);
        }
        return map;
    }

    /**
     * 
     * @param method
     * @param argMap
     * @return
     */
    private Object[] unmapArguments(Method method, Map<String, Object> argMap) {
        Parameter[] parameters = method.getParameters();
        Object[] args = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            args[i] = argMap.get(parameters[i].getName());
        }
        return args;
    }

    /**
     * Helper class to hold the input and output pipelines for a method.
     */
    private static class Pipeline {
    	
        private final List<IInputReactor> inputPipeline;
        private final List<IOutputReactor> outputPipeline;
        private final List<Map<String, Object>> inputParams; 
        private final List<Map<String, Object>> outputParams; 

        /**
         * 
         * @param inputPipeline
         * @param outputPipeline
         * @param inputParams
         * @param outputParams
         */
        Pipeline(List<IInputReactor> inputPipeline, List<IOutputReactor> outputPipeline, List<Map<String, Object>> inputParams, List<Map<String, Object>> outputParams) {
            this.inputPipeline = inputPipeline;
            this.outputPipeline = outputPipeline;
            this.inputParams = inputParams;
            this.outputParams = outputParams;
            
        }

        /**
         * 
         * @return
         */
        List<IInputReactor> getInputPipeline() {
            return inputPipeline;
        }

        /**
         * 
         * @return
         */
        List<IOutputReactor> getOutputPipeline() {
            return outputPipeline;
        }
        
        /**
         * 
         * @return
         */
        List<Map<String, Object>> getInputParams()
        {
        		return this.inputParams;
        }

        /**
         * 
         * @return
         */
        List<Map<String, Object>> getOutputParams()
        {
        		return this.outputParams;
        }
    }
    
}