package prerna.reactor.browser;

import java.io.File;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IBrowserEngine;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class ExecuteBrowserEngineReactor extends AbstractReactor {
	
	public ExecuteBrowserEngineReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Function Engine " + engineId + " does not exist or user does not have access to this function");
		}
		
		IBrowserEngine engine = Utility.getBrowserEngine(engineId);
		PlaywrightBrowserUtil pbu = this.insight.getPlaywrightUtil();
		if (pbu == null) {
			pbu = new PlaywrightBrowserUtil();
			this.insight.setPlaywrightUtil(pbu);
		}
		
		String base = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.BROWSER, engineId, engine.getEngineName());
		String fileName = engine.getBrowserFile();
		
		String path = base + File.separator + fileName;
		pbu.processFile(path);
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}}

