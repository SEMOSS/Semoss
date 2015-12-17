package prerna.nameserver;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.nlp.NLPSingletons;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.util.DIHelper;
import rita.RiWordNet;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;

public abstract class AbstractNameServer implements INameServer {
	
	protected static final Logger logger = LogManager.getLogger(ModifyMasterDB.class.getName());

	// defined name for master database
	protected String masterDBName = "MasterDatabase";

	protected IEngine masterEngine;
	protected LexicalizedParser lp;
	protected WordnetComparison wnComp;
	protected RiWordNet wordnet;
	
	
	/**
	 * Constructor for the class, using defined master database
	 */
	public AbstractNameServer() {
		this.masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
		
		// set up the wordnet and stanford nlp packages for 
		lp = NLPSingletons.getInstance().getLp();
		wordnet = NLPSingletons.getInstance().getWordnet();
		// creates the comparison class to determine similarity between concepts
		wnComp = new WordnetComparison();
	}
	
	/**
	 * Constructor for the class, using input database as master database
	 * Sometimes we do not need to use wordnet or stanford nlp library so avoid long loading time
	 * Modifies the name to match the input name for the master database
	 * @param masterEngine
	 */
	public AbstractNameServer(IEngine masterEngine) {
		this.masterEngine = masterEngine;
		this.masterDBName = masterEngine.getEngineName();
	}
	
	/**
	 * Constructor for the class, using input database as master database
	 * Defines the engine for the name server
	 * Defines the wordnet library
	 * Defines the stanford nlp library
	 */
	public AbstractNameServer(IEngine masterEngine, String wordNetDir, String lpDir) {
		this.masterEngine = masterEngine;
		
		// set up the wordnet and stanford nlp packages for 
		lp = NLPSingletons.getInstance().getLp();
		wordnet = NLPSingletons.getInstance().getWordnet();
		// creates the comparison class to determine similarity between concepts
		wnComp = new WordnetComparison();
	}
	
	public String getMasterDBName() {
		return masterDBName;
	}

	public void setMasterDBName(String masterDBName) {
		this.masterDBName = masterDBName;
	}

	public IEngine getMasterEngine() {
		return masterEngine;
	}

	public void setMasterEngine(BigDataEngine masterEngine) {
		this.masterEngine = masterEngine;
	}
}
