package prerna.reactor.utils;

import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;

public class ValidateRReactor extends AbstractReactor {
	
	public ValidateRReactor() {
		this.keysToGet = new String[]{"script"};
		this.keyRequired = new int[]{1};
	}

	@Override
	public NounMetadata execute() {

		organizeKeys();

		String result = "";
		
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(getLogger(this.getClass() + ""));
		rJavaTranslator.startR();
		
        String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
        String rFolder = baseFolder + "/R/util/smssutil.r";
        rFolder = rFolder.replaceAll("\\\\", "/");
		
        rJavaTranslator.runR("source('" + rFolder + "');" );

		try {
				String rScript = insight.getInsightFolder() + "/" +keyValue.get(keysToGet[0]);
				rScript = rScript.replaceAll("\\\\", "/");
				Object outObject = rJavaTranslator.executeR("canLoad('" + rScript + "')");
				
				String output = "";
				if(outObject instanceof org.rosuda.REngine.REXPString)
					output = ((org.rosuda.REngine.REXPString)outObject).asString();
				
				else if(outObject instanceof org.rosuda.JRI.REXP)
					output = ((org.rosuda.JRI.REXP)outObject).asString();

				output = output.replaceAll("\\\"", "");
					
				if(output.length() == 0)
					result = keyValue.get(keysToGet[0]) + " : All Libraries available";
				
				
				else
				{
					StringBuilder library = new StringBuilder(keyValue.get(keysToGet[0])).append(":  Missing Libraries [").append(output).append("]");
					result = library.toString();					
				}			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new NounMetadata(result, PixelDataType.CONST_STRING);
	}
}
