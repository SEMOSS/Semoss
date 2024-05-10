package prerna.engine.impl.model;

import prerna.engine.api.ModelTypeEnum;

public class EmbeddedModelEngine extends AbstractPythonModelEngine {
	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.EMBEDDED;
	}
	
//	public static void main(String [] args) throws Exception
//	{
//		String smssFilePath = "c:/users/pkapaleeswaran/workspacej3/SemossDev/db/PolicyBot.smss";
//		DIHelper.getInstance().loadCoreProp("c:/users/pkapaleeswaran/workspacej3/SemossDev/RDF_MAP.prop");
//		IModelEngine eng = new EmbeddedModelEngine();
//		eng.open(smssFilePath);
//		eng.startServer();
//		
//		Map <String, Object> params = new HashMap<String, Object>();
//		params.put("max_new_tokens", 2000);
//		params.put("temperature", 0.01);
//		
//		Map<String, Object> output = eng.ask("What is the capital of India ?", null, null, params);
//		//PyTranslator pyt = eng.getClient();
//		//System.err.println(output);
//		//Object output = pyt.runScript("i.ask(question='What is the capital of India ?')");
//		//System.err.println(output);
//	}
}
