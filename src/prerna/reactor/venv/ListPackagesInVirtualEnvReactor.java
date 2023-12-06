package prerna.reactor.venv;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVenvEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ListPackagesInVirtualEnvReactor extends AbstractReactor {
	
	public ListPackagesInVirtualEnvReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Virtual Environment " + engineId + " does not exist or user does not have access to it");
		}
		
		IVenvEngine engine = Utility.getVenvEngine(engineId);
		if (engine == null) {
			throw new SemossPixelException("Unable to find engine");
		}
		
		List<Map<String, String>> output;
		try {
			output = engine.listPackages();
		} catch (Exception e) {
			throw new SemossPixelException("Unable to run process to get package list");
		}
		
		return new NounMetadata(output, PixelDataType.VECTOR);
	}
}
