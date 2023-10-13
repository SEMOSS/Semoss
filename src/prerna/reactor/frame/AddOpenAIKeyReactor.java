package prerna.reactor.frame;

import java.util.HashMap;
import java.util.Map;

import prerna.ds.py.PyTranslator;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddOpenAIKeyReactor extends AbstractReactor {

	// if you ask without key
	// you will get if it is defined or not
	// if not you can add it
	String OPENAI_DEFINED = "openai_defined";
	
	public AddOpenAIKeyReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.API_KEY.getKey()};
		this.keyRequired = new int[] {0};
	}

	@Override
	public NounMetadata execute() {
		// do we need a way to check the library is installed?

		organizeKeys();
		PyTranslator pt = this.insight.getPyTranslator();
		if(keyValue.containsKey(keysToGet[0]))
		{
			// set the key
			String api_key = keyValue.get(keysToGet[0]);
			pt.runEmptyPy("import openai", "openai.api_key='"+ api_key + "'", OPENAI_DEFINED + "= True");
		}

		boolean output = (Boolean)pt.runScript("'" + OPENAI_DEFINED + "' in globals()");
	
		Map<String, Object> outMap = new HashMap<>();
		outMap.put(OPENAI_DEFINED, output);
		return new NounMetadata(outMap, PixelDataType.MAP);
	}

}
