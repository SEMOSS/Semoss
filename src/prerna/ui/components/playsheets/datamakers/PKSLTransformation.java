package prerna.ui.components.playsheets.datamakers;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

//import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc2.PKSLRunner;

public class PKSLTransformation extends AbstractTransformation {

	private static final Logger LOGGER = LogManager.getLogger(PKSLTransformation.class.getName());
	
	public static final String METHOD_NAME = "pksl";
	public static final String EXPRESSION = "EXPRESSION";

	private PKSLRunner runner;
	private List<String> parsedPksls = new Vector<String>();
//	private List<IPkqlMetadata> metadataList;

	IDataMaker dm;
	
	boolean addToRecipe = true;
	int recipeIndex = -1;
	
	public boolean isAddToRecipe() {
		return this.addToRecipe;
	}
	
	public int getRecipeIndex() {
		return this.recipeIndex;
	}

//	public List<IPkqlMetadata> getPkqlMetadataList() {
//		return this.metadataList;
//	}
	
	@Override
	public void setProperties(Map<String, Object> props) {
		//TODO: validate hash and set values
		this.props = props;
	}

	@Override
	public void setDataMakers(IDataMaker... dms){
		this.dm = (IDataMaker) dms[0];
	}

	@Override
	public void setDataMakerComponent(DataMakerComponent dmc){
		LOGGER.info("dmc is not needed for pksl");
	}

	@Override
	public void setTransformationType(Boolean preTransformation){
		LOGGER.info("pre transformation is not needed for pksl");
	}

	@Override
	public void runMethod() {

		// check how long runner response array is
		int numOldCmds = runner.getResults().size();
		
		String expression = props.get(EXPRESSION) + "";		
		runner.runPKSL(expression, (IDataMaker) this.dm);
				
//		this.dm = runner.getDataFrame();
		
//		this.feData.putAll(runner.getFeData());
		
//		if(runner.getNewColumns() != null) {
//			this.newColumns.putAll(runner.getNewColumns());
//		}
		
		// running the pkql may have changed the datamaker:::::::::::::::::::::::::::::::::::::::::::::::::::::
		// need to remember to set this back into the insight:::::::::::::::::::::::::::::::::::::::::::::::::
//		this.dm = runner.getDataFrame();
		
		// store added responses
		List<Map> allCmds = runner.getResults();
		for(int i = numOldCmds ; i < allCmds.size(); i++){
			String cmd = (String) allCmds.get(i).get("command");
			if(cmd != null){
				if(cmd.startsWith("v:") || cmd.startsWith("data.query")) {
					if(!cmd.contains("user.input")) {
						this.addToRecipe = false;
					} else {
						this.recipeIndex = 0;
					}
				} 
				parsedPksls.add(cmd);
			}
			else {
				LOGGER.error("this is weird... my runner response doesn't have a PKSL command stored. Skipping for now in terms of adding to recipe");
			}
		}
		
		// store the metadata list on the post transformation
		// this will be consolidated at the insight lvl
		// but since insight runs all the pkql post transformations at the same 
		// time on the datamaker
		// i want to separate each one out since i dont want to have to constantly
		// loop through everything in order to determine the difference
		// would be unnecessary operation to perform when doing through create
//		this.metadataList = runner.getMetadataResponse();
//		if(this.metadataList != null) {
//			for(IPkqlMetadata meta : metadataList) {
//				meta.setInvokingPkslTransformation(this);
//			}
//		}
	}

	@Override
	public Map<String, Object> getProperties() {
		props.put(TYPE, METHOD_NAME);
		return this.props;
	}

	@Override
	public void undoTransformation() {
		LOGGER.info("unable to undo pksl transformation");
	}

	@Override
	public PKSLTransformation copy() {
		return this;
	}
	
//	public Map<String, Object> getFeData(){
//		return this.feData;
//	}
	
//	public Map<String, String> getNewColumns() {
//		return this.newColumns;
//	}
	
	public void setRunner(PKSLRunner runner){
		this.runner = runner;
	}
	
	
	public List<String> getPksl() {
		return parsedPksls;
	}
	
	public void setPkql(List<String> parsedPksls) {
		this.parsedPksls = parsedPksls;
	}
}
