package prerna.reactor;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class EchoEReactor extends AbstractReactor {

	public EchoEReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.VALUE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String val = null;
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			val = grs.getNoun(0).getValue()+"";
		}
		grs = this.store.getNoun(ALL_NOUN_STORE);
		if(grs != null && !grs.isEmpty()) {
			val = grs.getNoun(0).getValue()+"";
		}
		
		return new NounMetadata(Utility.decodeURIComponent(val), PixelDataType.CONST_STRING);
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.VALUE.getKey())) {
			return "The value to echo back";
		}
		return super.getDescriptionForKey(key);
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor returns back the encoded input the user enters";
	}
}
