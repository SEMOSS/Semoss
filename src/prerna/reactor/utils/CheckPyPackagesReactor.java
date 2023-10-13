package prerna.reactor.utils;

import java.util.HashMap;
import java.util.Map;

import prerna.ds.py.PyUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CheckPyPackagesReactor extends AbstractReactor {

	// keep the list of packages static so 
	// we do not need to call this every time
	public static String[] pkgs = null;
	public static boolean pyInstalled = false;

	public CheckPyPackagesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.RELOAD.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String reloadStr = this.keyValue.get(this.keysToGet[0]);
		boolean reload = reloadStr != null && Boolean.parseBoolean(reloadStr);
		setPackages(reload);
		Map<String, Object> returnMap = new HashMap<String,Object>();
		returnMap.put("pyInstalled", CheckPyPackagesReactor.pyInstalled);
		returnMap.put("PY", CheckPyPackagesReactor.pkgs);
		return new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CHECK_R_PACKAGES);
	}
	
	private void setPackages(boolean reload) {
		if(!PyUtils.pyEnabled()) {
			CheckPyPackagesReactor.pyInstalled = false;
			CheckPyPackagesReactor.pkgs = new String[0];
		}
		
		if(reload || pkgs == null) {
			try{
				pkgs = this.insight.getPyTranslator().getStringArray("smssutil.getalllibraries()");
				CheckPyPackagesReactor.pyInstalled = true;
			} catch(Exception e){
				CheckPyPackagesReactor.pyInstalled = false;
				CheckPyPackagesReactor.pkgs = new String[0];
			}
		}
	}
}
