//package prerna.algorithm.nlp;
//
//import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//import rita.RiWordNet;
//
//public class NLPSingletons {
//
//	private static NLPSingletons singleton;
//
//	private LexicalizedParser lp;
//	private RiWordNet wordnet;
//	
//	private NLPSingletons() {
//		String wordNetDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
//				+ System.getProperty("file.separator") + "WordNet-3.1";
//		String nlpPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
//				+ System.getProperty("file.separator") + "NLPartifacts" + System.getProperty("file.separator")
//				+ "englishPCFG.ser";
//		
//		lp = LexicalizedParser.loadModel(nlpPath);
//		lp.setOptionFlags(new String[]{"-maxLength", "80", "-retainTmpSubcategories"});
//		wordnet = new RiWordNet(wordNetDir, false, true); // params: wordnetInstallDir, ignoreCompoundWords, ignoreUppercaseWords
//	}
//	
//	public static NLPSingletons getInstance() {
//		if(singleton == null) {
//			singleton = new NLPSingletons();
//		}
//		return singleton;
//	}
//	
//	public LexicalizedParser getLp() {
//		return this.lp;
//	}
//	
//	public RiWordNet getWordnet() {
//		return this.wordnet;
//	}
//}
