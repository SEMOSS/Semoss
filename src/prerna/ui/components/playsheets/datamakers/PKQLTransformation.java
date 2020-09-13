package prerna.ui.components.playsheets.datamakers;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc.PKQLRunner;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc2.PixelRunner;

public class PKQLTransformation extends AbstractTransformation {

	private static final Logger LOGGER = LogManager.getLogger(PKQLTransformation.class.getName());
	
	public static final String METHOD_NAME = "pkql";
	public static final String EXPRESSION = "EXPRESSION";

	private PKQLRunner runner;
	private PixelRunner runner2;
	private List<String> parsedPkqls = new Vector<String>();
	private List<IPkqlMetadata> metadataList;

//	private Map<String, Object> feData = new HashMap<String, Object>();
//	private Map<String, String> newColumns = new HashMap<String, String>();
	
	IDataMaker dm;
	
	boolean addToRecipe = true;
	int recipeIndex = -1;
	
	public boolean isAddToRecipe() {
		return this.addToRecipe;
	}
	
	public int getRecipeIndex() {
		return this.recipeIndex;
	}

	public List<IPkqlMetadata> getPkqlMetadataList() {
		return this.metadataList;
	}
	
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
		LOGGER.info("dmc is not needed for pkql");
	}

	@Override
	public void setTransformationType(Boolean preTransformation){
		LOGGER.info("pre transformation is not needed for pkql");
	}

	@Override
	public void runMethod() {
//		if(runner == null) {
//			String expression = props.get(EXPRESSION) + "";	
//			runner2.runPixel(expression, this.dm);
//			return;
//		}
		// check how long runner response array is
		int numOldCmds = runner.getResults().size();
		
		String expression = props.get(EXPRESSION) + "";		
		runner.runPKQL(expression, (IDataMaker) this.dm);
				
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
				parsedPkqls.add(cmd);
			}
			else {
				LOGGER.error("this is weird... my runner response doesn't have a PKQL command stored. Skipping for now in terms of adding to recipe");
			}
		}
		
		// store the metadata list on the post transformation
		// this will be consolidated at the insight lvl
		// but since insight runs all the pkql post transformations at the same 
		// time on the datamaker
		// i want to separate each one out since i dont want to have to constantly
		// loop through everything in order to determine the difference
		// would be unnecessary operation to perform when doing through create
		this.metadataList = runner.getMetadataResponse();
		if(this.metadataList != null) {
			for(IPkqlMetadata meta : metadataList) {
				meta.setInvokingPkqlTransformation(this);
			}
		}
	}

	@Override
	public Map<String, Object> getProperties() {
		props.put(TYPE, METHOD_NAME);
		return this.props;
	}

	@Override
	public void undoTransformation() {
		LOGGER.info("unable to undo pkql transformation");
	}

	@Override
	public PKQLTransformation copy() {
		return this;
//		PKQLTransformation joinCopy = new PKQLTransformation();
//		joinCopy.setDataMakers(dm);
//		joinCopy.setId(id);
//		joinCopy.runner = this.runner; // keep this shallow so updates can be gotten
//		joinCopy.feData = this.feData; // keep this shallow so updates can be gotten
//		joinCopy.parsedPkqls = this.parsedPkqls; // keep this shallow so updates can be gotten
//
//		if(props != null) {
//			Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
//			String propCopy = gson.toJson(props);
//			Map<String, Object> newProps = gson.fromJson(propCopy, new TypeToken<Map<String, Object>>() {}.getType());
//			joinCopy.setProperties(newProps);
//		}
//
//		return joinCopy;
	}
	
//	public Map<String, Object> getFeData(){
//		return this.feData;
//	}
	
//	public Map<String, String> getNewColumns() {
//		return this.newColumns;
//	}
	
	public void setRunner(PKQLRunner runner){
		this.runner = runner;
	}
	
	public void setRunner(PixelRunner runner) {
		this.runner2 = runner;
	}
	
	
	public List<String> getPkql() {
		return parsedPkqls;
	}
	
	public void setPkql(List<String> parsedPkqls) {
		this.parsedPkqls = parsedPkqls;
	}
}
