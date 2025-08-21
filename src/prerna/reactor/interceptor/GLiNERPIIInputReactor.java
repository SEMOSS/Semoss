package prerna.reactor.interceptor;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.guardrail.GLiNERGuardrailEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GLiNERPIIInputReactor extends AbstractReactor implements IInputReactor {

	private static final Logger classLogger = LogManager.getLogger(GLiNERPIIInputReactor.class);

	public GLiNERPIIInputReactor() {
		this.keysToGet = new String[]{ PipelineReactorUtils.ARGUMENTS };
	}

	@Override
	public NounMetadata execute() { 
		GenRowStruct grs = this.getNounStore().getNoun(keysToGet[0]);
		Map <String, Object> arguments = new HashMap<String, Object>();
		if(grs != null && grs.size() > 0) {
			arguments = (Map<String, Object>) grs.get(0);
			Method method = (Method) arguments.get(PipelineReactorUtils.METHOD_NAME);
			String methodName = method.getName();
			
			Map <String, Object> config = (Map<String, Object>) arguments.get(PipelineReactorUtils.CONFIG);
			String glinerEId = (String) config.get("gliner_engineid");
			if(glinerEId == null) {
				throw new SecurityException("Interceptor is not configured correctly. Please reach out to engine owner");
			}
			GLiNERGuardrailEngine gliner = (GLiNERGuardrailEngine) Utility.getEngine(glinerEId);
			NounStore glinerNounStore = new NounStore("gliner");
			if (config.containsKey("piiTypes")) {
				List<String> piiTypes = (List<String>) config.get("piiTypes");
				GenRowStruct labelsGrs = glinerNounStore.makeNoun("labels");
				for(String piiType : piiTypes) {
					labelsGrs.add(new NounMetadata(piiType, PixelDataType.CONST_STRING));
				}
			}
			
			boolean blockOnPII = true;
			if (config.containsKey("blockOnPII")) {
				blockOnPII = (boolean) config.get("blockOnPII");
			}
			
			IEngine engine = (IEngine) arguments.get(PipelineReactorUtils.ENGINE);
			if (engine instanceof IModelEngine && methodName.equals("ask")) {
				String question = (String) arguments.get("arg0");
				if (question != null) {
					glinerNounStore.makeNoun("prompt").add(new NounMetadata(question, PixelDataType.CONST_STRING));
				}
			}
			
			GuardrailNounMetadata output = (GuardrailNounMetadata) gliner.execute(glinerNounStore, null);
			
			Map <String, Object> resultMap = new HashMap<String, Object>();
			resultMap.put(PipelineReactorUtils.INTERCEPTOR, this.getClass().getName());
			resultMap.put(PipelineReactorUtils.PASS, output.isPass());
			arguments.put(PipelineReactorUtils.INTERIM_RESULT, resultMap);
			return new NounMetadata(arguments, PixelDataType.MAP);
		}
		
		Map <String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put(PipelineReactorUtils.INTERCEPTOR, this.getClass().getName());
		resultMap.put(PipelineReactorUtils.PASS, true);
		// add this interim result back into arguments...?
		arguments.put(PipelineReactorUtils.INTERIM_RESULT, resultMap);
		return new NounMetadata(arguments, PixelDataType.MAP);
	}
}