package prerna.engine.impl.model;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;

public class BedrockEngine extends AbstractPythonModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(BedrockEngine.class);

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.BEDROCK;
	}
	
	/**
	 * 
	 * @param filePath
	 * @param insight
	 * @param parameters
	 * @return
	 */
	public AskModelEngineResponse summarize(String filePath, Insight insight, Map<String, Object> parameters) {
		checkSocketStatus();
		
		StringBuilder callMaker = new StringBuilder(this.varName + ".summarize(");	
		if (filePath != null) {
			callMaker.append("file_path")
					 .append("=")
					 .append(PyUtils.determineStringType(filePath));
		} else {
			throw new IllegalArgumentException("No file path given to summarize");
		}
		
		callMaker.append(")");
		classLogger.debug("Running >>>" + callMaker.toString());
		
		Object output = pyt.runScript(callMaker.toString(), insight);
		AskModelEngineResponse response = AskModelEngineResponse.fromObject(output);
		return response;
	}

	/**
	 * 
	 * @param filePath
	 * @param insight
	 * @return
	 */
	public EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight) {
		checkSocketStatus();
		
		StringBuilder callMaker = new StringBuilder(this.varName + ".embeddings_call(");	
		if (stringsToEmbed != null && !stringsToEmbed.isEmpty()) {
			callMaker.append("strings_to_embed")
					 .append("=")
					 .append(PyUtils.determineStringType(stringsToEmbed));
		} else {
			throw new IllegalArgumentException("Nothing given to embed");
		}
		
		callMaker.append(")");
		classLogger.debug("Running >>>" + callMaker.toString());
		
		Object output = pyt.runScript(callMaker.toString(), insight);
		EmbeddingsModelEngineResponse response = AskModelEngineResponse.fromObject(output);
		return response;
	}
	
}
