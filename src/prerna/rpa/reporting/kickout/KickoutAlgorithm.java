package prerna.rpa.reporting.kickout;

import java.util.HashMap;
import java.util.Map;

public enum KickoutAlgorithm {
	OPEN_RECORDS("open_records");
	
	private final String algorithmName;
	
	private static final Map<String, KickoutAlgorithm> ALGORITHM_NAME_TO_KICKOUT_ALGORITHM = new HashMap<>();
	
	// Map the algorithm name to the KickoutAlgorithm
	static {
		for (KickoutAlgorithm kickoutAlgorithm : KickoutAlgorithm.values()) {
			ALGORITHM_NAME_TO_KICKOUT_ALGORITHM.put(kickoutAlgorithm.getAlgorithmName(), kickoutAlgorithm);
		}
	}
	
	KickoutAlgorithm(final String algorithmName) {
		this.algorithmName = algorithmName;
	}
	
	public static KickoutAlgorithm getKickoutAlgorithmFromAlgorithmName(String algorithmName) {
		return ALGORITHM_NAME_TO_KICKOUT_ALGORITHM.get(algorithmName);
	}
	
	public String getAlgorithmName() {
		return algorithmName;
	}

}
