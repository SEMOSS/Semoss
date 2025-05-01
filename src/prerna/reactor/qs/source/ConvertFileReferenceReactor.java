package prerna.reactor.qs.source;

import java.nio.file.Path;
import java.nio.file.Paths;

import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ConvertFileReferenceReactor extends AbstractReactor {

	private final String FILE_KEY = "fileKey";
	
	public ConvertFileReferenceReactor() {
		this.keysToGet = new String[] { FILE_KEY };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		InsightFile insightFile = this.insight.getExportInsightFiles().get(this.keyValue.get(this.keysToGet[0]));
		Path insightPath = Paths.get(this.insight.getInsightFolder()).toAbsolutePath();
		Path insightFilePath = Paths.get(insightFile.getFilePath()).toAbsolutePath();
		
		Path relativePath = insightPath.relativize(insightFilePath);
		return new NounMetadata(relativePath.toString(), PixelDataType.CONST_STRING);
	}
	
}