package prerna.io.connector.antivirus;

import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

public interface IVirusScanner {
	
	@Deprecated
	String CLAM_AV = "ClamAV";
	
	enum VIRUS_SCANNER_TYPE
	{
		CLAM_AV,
		VIRUS_TOTAL,
	}
	
	Map<String, Collection<String>> getViruses(String name, InputStream is);

}
