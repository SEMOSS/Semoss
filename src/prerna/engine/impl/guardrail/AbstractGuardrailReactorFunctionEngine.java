package prerna.engine.impl.guardrail;

import java.util.Properties;

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

}
