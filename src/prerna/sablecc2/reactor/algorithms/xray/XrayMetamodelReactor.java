package prerna.sablecc2.reactor.algorithms.xray;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;

public class XrayMetamodelReactor extends AbstractRFrameReactor {
	private static final String CLASS_NAME = XrayMetamodelReactor.class.getName();

	public XrayMetamodelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.CONFIG.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		Logger logger = getLogger(CLASS_NAME);

		// need to make sure that the textreuse package is installed
		logger.info("Checking if required R packages are installed to run X-ray...");
		this.rJavaTranslator.checkPackages(new String[]{"textreuse", "digest", "memoise", "withr", "jsonlite"});

		organizeKeys();
		// get metamodel
		System.out.println("");
		GenRowStruct grs = this.store.getNoun(keysToGet[0]);
		Map<String, Object> config = null;
		if(grs != null && !grs.isEmpty()) {
			config = (Map<String, Object>) grs.get(0);
		} else {
			throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.CONFIG.getKey());
		}
		
		Xray xray = new Xray(this.rJavaTranslator, getBaseFolder(), logger);
		String rFrameName = xray.run(config);
		
		// format xray results into json

		return new NounMetadata(rFrameName, PixelDataType.CONST_STRING);
	}

}
