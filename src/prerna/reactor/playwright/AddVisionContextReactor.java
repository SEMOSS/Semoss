package prerna.reactor.playwright;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddVisionContextReactor extends AbstractReactor {
	
	
	public AddVisionContextReactor() {
		this.keysToGet = new String[] { "visionContext" };
		this.keyRequired = new int[] { 1 }; 
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String visionContext = this.keyValue.get("visionContext");

		
		if (visionContext == null) {
			throw new IllegalArgumentException("Vision context is null");
		}

		return new NounMetadata(visionContext, PixelDataType.CONST_STRING);
	}
	
	@Override
	public String getReactorDescription() {
		return "Reactor to return vision context";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("visionContext")) {
			return "Context from the vision model";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}