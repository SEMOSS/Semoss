package prerna.engine.impl.guardrail;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.NotImplementedException;

import prerna.engine.api.IEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.engine.impl.function.AbstractReactorFunctionEngine;

public abstract class AbstractGuardrailReactorFunctionEngine extends AbstractReactorFunctionEngine implements IGuardrailReactorFunctionEngine {

	@Override
	public CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.GUARDRAIL;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return "GUARDRAIL";
	}
	
	@Override
	public Map<String, Object> buildOpenAIFunctionEngineToolMap() {
		throw new NotImplementedException("This method has not been implemented yet...");
	}

}
