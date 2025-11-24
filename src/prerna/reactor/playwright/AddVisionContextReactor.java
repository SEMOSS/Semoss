package prerna.reactor.playwright;

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
		return "Reactor to return extracted contexts from the playwright app to the playground ";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("visionContext")) {
			return "The context from the playwright app that we need to return to the playground";
		}

		return super.getDescriptionForKey(key);
	}
}