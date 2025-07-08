package prerna.reactor.project;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.AbstractPythonModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class EnginePyReactor extends AbstractReactor  {
	
	private static final Logger classLogger = LogManager.getLogger(EnginePyReactor.class);

	
	public EnginePyReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.CODE.getKey(), ReactorKeysEnum.ENGINE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		
		if(engineId == null || (engineId=engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}
		
		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have editor access to this model");
		}
		
		String code = Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.CODE.getKey()));
		if (code == null || code.trim().isEmpty()) {
			throw new IllegalArgumentException("Code parameter cannot be null or empty");
		}
		
		IEngine rawEngine = Utility.getEngine(engineId);
		if (!(rawEngine instanceof AbstractPythonModelEngine)) {
		    throw new IllegalArgumentException("Engine " + engineId + " is not a Python model engine");
		}
		AbstractPythonModelEngine engine = (AbstractPythonModelEngine) rawEngine;
		
		PyTranslator enginePyTranslator = null;
		String output = null;
		
		try {
			enginePyTranslator = engine.getEnginePyTranslator();
			output = enginePyTranslator.runSingle(code);
		} catch (IllegalArgumentException e) {
			classLogger.warn("Invalid argument when getting PyTranslator for engine {}: {}", engineId, e.getMessage());
			throw e;
		} catch (IllegalStateException e) {
			classLogger.error("Engine {} is not properly initialized or connection failed: {}", engineId, e.getMessage());
			throw new IllegalArgumentException("Engine " + engineId + " is currently unavailable. Please try again later.", e);
			
		} catch (Exception e) {
			classLogger.error("Unexpected error executing code on engine {}: {}", engineId, e.getMessage(), e);
			throw new IllegalArgumentException("Failed to execute code on engine " + engineId + ": " + e.getMessage(), e);
		}
		
		if (output == null) {
			classLogger.warn("Code execution returned null output for engine {}", engineId);
			output = "";
		}
		
		List<NounMetadata> outputs = new ArrayList<>(1);
		outputs.add(new NounMetadata(output, PixelDataType.CONST_STRING));
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}
}
