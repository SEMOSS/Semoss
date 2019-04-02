package prerna.sablecc2.reactor.algorithms.dataquality;

import java.util.Map;

import com.google.gson.Gson;

import edu.stanford.nlp.io.IOUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class GetDataQualityRulesReactor extends AbstractReactor{

	private static final String DIR_SEP = System.getProperty("file.separator");
	
	public NounMetadata execute() {
		String base = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String fileLoc = base + DIR_SEP + "R" + DIR_SEP + "DQ" + DIR_SEP + "rule-defs.json";
		System.out.println(fileLoc);
		String fileString = IOUtils.stringFromFile(fileLoc);
		Gson gson = new Gson();
		Map<String, Object> rulesMap = gson.fromJson(fileString, Map.class);
		NounMetadata noun = new NounMetadata(rulesMap, PixelDataType.MAP);

		return noun;
	}
}