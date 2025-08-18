package prerna.engine.impl.function;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;

public class LocalPythonCustomEmbeddingsFunctionEngine extends LocalPythonFunctionEngine implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(LocalPythonCustomEmbeddingsFunctionEngine.class);

	private static final String CAN_PROCESS_FUNCTION_NAME = "CAN_PROCESS_FUNCTION_NAME";
	private String canProcessFunctionName = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		this.canProcessFunctionName = this.smssProp.getProperty(CAN_PROCESS_FUNCTION_NAME);
	}

	@Override
	public boolean canProcessDocument(File fileToProcess) {
		if(this.canProcessFunctionName == null || (this.canProcessFunctionName=this.canProcessFunctionName.trim()).isEmpty()) {
			return true;
		}
		
		checkSocketStatus();

		StringBuilder callMaker = new StringBuilder(this.canProcessFunctionName);
					callMaker.append("(fileToProcess=")
						.append(PyUtils.determineStringType(fileToProcess))
						.append("')");
		
		return (boolean) pyTranslator.runDirectPy(callMaker.toString());
	}

	@Override
	public int processDocument(String outputCsvFilePath, File fileToProcess, Map<String, Object> parameters) {
		checkSocketStatus();

		StringBuilder callMaker = new StringBuilder(this.functionName);
					callMaker.append("(outputCsvFilePath=").append(PyUtils.determineStringType(outputCsvFilePath))
							.append(",fileToProcess=").append(PyUtils.determineStringType(fileToProcess))
							.append(",parameters=").append(PyUtils.determineStringType(parameters))
							.append(")");

		return ((Number) pyTranslator.runDirectPy(callMaker.toString())).intValue();
	}
	
	@Override
	public Object execute(Map<String, Object> parameterValues) {
		throw new IllegalArgumentException("This function engine is only intended to be executed for custom vector db embeddings");
	}
	
	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.LOCAL_PYTHON_CUSTOM_EMBEDDINGS.name();
	}
}
