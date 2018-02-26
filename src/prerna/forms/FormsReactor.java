package prerna.forms;

import java.io.IOException;
import java.util.Map;

import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class FormsReactor extends AbstractReactor {

	private static final String FORM_DATA = "form_input";
	
	public FormsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), FORM_DATA};
	}
	
	@Override
	public NounMetadata execute() {
		String databaseName = this.store.getNoun(this.keysToGet[0]).get(0) + "";
		Map<String, Object> engineHash = (Map<String, Object>) this.store.getNoun(FORM_DATA).get(0);
		
		IEngine engine = Utility.getEngine(databaseName);
		AbstractFormBuilder formbuilder = FormFactory.getFormBuilder(engine);
		try {
			formbuilder.commitFormData(engineHash);
		} catch (IOException e) {
			e.printStackTrace();
			return new NounMetadata(false, PixelDataType.BOOLEAN);
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
