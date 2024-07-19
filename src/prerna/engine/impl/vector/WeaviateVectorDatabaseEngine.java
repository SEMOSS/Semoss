package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.internal.LinkedTreeMap;

import io.weaviate.client.Config;
import io.weaviate.client.WeaviateAuthClient;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.auth.exception.AuthException;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.model.BatchDeleteResponse;
import io.weaviate.client.v1.data.model.WeaviateObject;
import io.weaviate.client.v1.filters.Operator;
import io.weaviate.client.v1.filters.WhereFilter;
import io.weaviate.client.v1.graphql.model.GraphQLResponse;
import io.weaviate.client.v1.graphql.query.argument.NearVectorArgument;
import io.weaviate.client.v1.graphql.query.fields.Field;
import io.weaviate.client.v1.schema.model.Schema;
import io.weaviate.client.v1.schema.model.WeaviateClass;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;

public class WeaviateVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(WeaviateVectorDatabaseEngine.class);

	public static final String WEAVIATE_CLASSNAME = "WEAVIATE_CLASSNAME";
	public static final String AUTOCUT = "AUTOCUT";
	
	private WeaviateClient client = null;
	private String host = null;
	private String protocol = "https";
	private String apiKey = null;
	
	private String className = null;
	private int autocut = 1;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		this.host = smssProp.getProperty(Constants.HOSTNAME);
		if(this.host == null || (this.host=this.host.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the host");
		}
		
		if(this.host.startsWith("https://")) {
			this.host = this.host.substring("https://".length(), host.length());
		} else if(this.host.startsWith("http://")) {
			this.protocol = "http";
			this.host = this.host.substring("http://".length(), host.length());
		}
		
		this.apiKey = smssProp.getProperty(Constants.API_KEY);
		if(this.apiKey == null || (this.apiKey=this.apiKey.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the api key");
		}
		
		this.className = smssProp.getProperty(WEAVIATE_CLASSNAME);
		
		connect2Weviate(this.protocol, this.host, this.apiKey);
		createClass(this.className);
		
		String autoCutStr = smssProp.getProperty(AUTOCUT);
		if(autoCutStr != null && !(autoCutStr=autoCutStr.trim()).isEmpty()) {
			try {
				this.autocut = Integer.parseInt(autoCutStr);
			} catch(NumberFormatException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Invalid input for autocut. Must be a positive integer value. Value was: " + autoCutStr);
			}
		}
	}
	
	/**
	 * 
	 * @param protocol
	 * @param host
	 * @param apiKey
	 */
	private void connect2Weviate(String protocol, String host, String apiKey) {
		try {
			Config config = new Config(protocol, host);
			this.client = WeaviateAuthClient.apiKey(config, apiKey);
		} catch (AuthException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * 
	 * @param className
	 */
	private void createClass(String className) {
		// check to see if the class is available
		// if not create a class
		Schema s = client.schema().getter().run().getResult();
		if(s == null) {
			throw new IllegalArgumentException("Unable to pull the weaviate schema");
		}
		
		List<WeaviateClass> classes = s.getClasses();
		boolean foundClass = false;
		for(int classIndex = 0; !foundClass && classIndex < classes.size(); classIndex++) {
			WeaviateClass wc = classes.get(classIndex);
			String curName = wc.getClassName();
			foundClass = curName.equalsIgnoreCase(className);
		}
		
		if(!foundClass) {
			// see if the properties have been passed
			// if not create it
			//String [] classProps = 
			WeaviateClass emptyClass = WeaviateClass.builder()
					  .className(className)
					  .build();
			// Add the class to the schema
			Result<Boolean> result = client.schema().classCreator()
					  .withClass(emptyClass)
					  .run();
		}
	}

	@Override
	public void addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight, Map<String, Object> parameters) throws Exception {
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		
		// if we were able to extract files, begin embeddings process
		IModelEngine embeddingsEngine = Utility.getModel(this.embedderEngineId);
		
		// send all the strings to embed in one shot
		vectorCsvTable.generateAndAssignEmbeddings(embeddingsEngine, insight);

		ObjectsBatcher batcher = client.batch().objectsBatcher();
		for(int rowIndex = 0; rowIndex < vectorCsvTable.rows.size(); rowIndex++) {
			VectorDatabaseCSVRow row = vectorCsvTable.getRows().get(rowIndex);
			Map<String, Object> properties = new HashMap<>();
			properties.put("Source", row.getSource());  
			properties.put("Modality", row.getModality());  
			properties.put("Divider", row.getDivider());  
			properties.put("Part", row.getPart());  
			properties.put("Tokens", row.getTokens());  
			properties.put("Content", row.getContent());

			List<? extends Number> embedding = row.getEmbeddings();
			Float[] vector = new Float[embedding.size()];
			for(int vecIndex = 0;vecIndex < vector.length; vecIndex++) {
				vector[vecIndex] = embedding.get(vecIndex).floatValue();
			}

			batcher.withObject(WeaviateObject.builder()
					.className(className)
					.properties(properties)
					.vector(vector)
					.build()
					);
		}
		batcher.run();
	}
	
	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		List<String> filesToRemoveFromCloud = new ArrayList<String>();
		// need to get the source names and then delete it based on the names
		for(int fileIndex = 0;fileIndex < fileNames.size();fileIndex++) {
			String fileName = fileNames.get(fileIndex);
			
			BatchDeleteResponse  result = client.batch().objectsBatchDeleter()
					.withClassName(className)
					.withWhere(WhereFilter.builder()
							.path("source")
							.operator(Operator.Equal)
							.valueText(fileName)
							.build())
					.run().getResult();
			
			//{'dryRun': False, 'match': {'class': 'Vector_Table', 'where': {'operands': None, 'operator': 'Like', 'path': ['content'], 'valueString': 'interpreted very broadly'}}, 'output': 'minimal', 'results': {'failed': 0, 'limit': 100000, 'matches': 1, 'objects': None, 'successful': 1}}
			long success = result.getResults().getSuccessful();
			classLogger.info("Deleted File " + fileName + " <> " + (success == 1));
			
			String documentName = Paths.get(fileName).getFileName().toString();
			// remove the physical documents
			File documentFile = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "documents", documentName);
			try {
				if (documentFile.exists()) {
					FileUtils.forceDelete(documentFile);
					filesToRemoveFromCloud.add(documentFile.getAbsolutePath());
				}
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}

		}
		
		if (ClusterUtil.IS_CLUSTER) {
			Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(), filesToRemoveFromCloud.stream().toArray(String[]::new)));
			deleteFilesFromCloudThread.start();
		}
	}

	@Override
	public List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit, Map<String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		if(limit == null) {
			limit = 3;
		}
		int cutter = autocut;
		if(parameters.containsKey(AUTOCUT)) {
			cutter = Integer.parseInt(parameters.get(AUTOCUT) + "");
		}
		
		List<Map<String, Object>> retOut = new ArrayList<>();
		
		Float [] vector = getEmbeddingsFloat(searchStatement, insight);

		Field content = Field.builder().name("content").build();
		Field source = Field.builder().name("source").build();
		Field divider = Field.builder().name("divider").build();
		Field part = Field.builder().name("part").build();
		Field modality = Field.builder().name("modality").build();
		
		Field _additional = Field.builder()
				.name("_additional")
				.fields(new Field[]{
						Field.builder().name("certainty").build(),  // only supported if distance==cosine
						Field.builder().name("distance").build()   // always supported
				}).build();

		NearVectorArgument nearVector = NearVectorArgument.builder().vector(vector).build();
		
		GraphQLResponse response = client.graphQL().get().withClassName(className)
				.withFields(content, source, divider, part, modality, _additional)
				.withNearVector(nearVector)
				.withAutocut(cutter)
				.withLimit(limit.intValue())
				.run()
				.getResult();

		// hashmap = LinkedTreeMap
		// each level is a hashmap
		// get is first level
		// followed by vector table
		// followed list of contents - array list
		// the contents is again another hashmap
		
		// get
		LinkedTreeMap getMap = (LinkedTreeMap)((LinkedTreeMap)response.getData()).get("Get");
		
		// vector table
		ArrayList outputs = (ArrayList)getMap.get(className);

		// each of the output is another treemap with each of the fields.. yay 
		for(int outputIndex = 0;outputIndex < outputs.size();outputIndex++) {
			LinkedTreeMap thisOutput = (LinkedTreeMap)outputs.get(outputIndex);
			LinkedTreeMap additional = (LinkedTreeMap)thisOutput.get("_additional");
			Map <String, Object> outputMap = new HashMap();
			outputMap.put("Source", thisOutput.get("source"));
			outputMap.put("Divider", thisOutput.get("divider"));
			outputMap.put("Modality", thisOutput.get("modality"));
			outputMap.put("Part", thisOutput.get("part"));
			outputMap.put("Content", thisOutput.get("content"));
			outputMap.put("Score", additional.get("certainty"));
			outputMap.put("Distance", additional.get("distance"));
			retOut.add(outputMap);
		}
		return retOut;
	}
	
	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.WEAVIATE;
	}
	
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	
	
//	public static void main(String [] args)
//	{
//		WeaviateVectorDatabaseEngine w = new WeaviateVectorDatabaseEngine();
//		String apiKey = "Xxxxxxxxx";
//		String host = "wsandbox-13hnmiur.weaviate.network";
//		
//		w.connect2Weviate("https", host, apiKey);
//		
//		Meta meta = w.client.misc().metaGetter().run().getResult();
//		
//        // Print the meta information
//        System.out.println(meta);
//		
//		//w.client.schema().classGetter().run();
//		Result <Meta> metas = w.client.misc().metaGetter().run();
//		
//		Schema s = w.client.schema().getter().run().getResult();
//		
//		System.err.println(s);
//		
//		WeaviateClass thisClass = w.client.schema().classGetter().withClassName("Document").run().getResult();
//		
//		System.err.println(thisClass);
//		
//		//["can I buy car as gift for my client"]
//		double [] qvector = new double[] {-0.008417927, 0.036340643, -0.028145125, -0.01415344, -0.004852608, -0.035720587, 0.021391913, 0.045452762, 0.01792769, 0.020785337, 0.010884669, 0.019599143, 0.028441673, 0.0077978713, -0.00053749373, 0.01288637, 0.043053415, 0.012212397, -0.010338751, 0.017563745, 0.004829019, 0.026864577, -0.065564126, -0.05227337, -0.026001891, 0.021108845, 0.023184681, -0.03911741, 0.076563366, 0.001592262, -0.06696599, -0.004067429, 0.00061415817, -0.0155418245, -0.0257997, 0.012070863, 0.011444067, -0.021459311, -0.029142607, -0.021823255, -0.002328578, -0.0015627756, 0.023845175, 0.019639583, -0.061789874, -0.02073142, -0.00039616995, -0.08384228, -0.025503151, -0.07187252, 0.005179485, 0.016876291, -0.011140779, 0.0073126103, 0.028199043, -0.02655455, 0.012529164, -0.016027085, -0.020920131, 0.03302469, 0.020866213, -0.00833705, 0.0059848833, -0.013668179, 0.0046099774, 0.045614514, -0.00089217216, -0.012906589, 0.017563745, -0.016566264, -0.01415344, -7.245213e-05, -0.0139916865, 0.032701187, 0.025085287, 0.005283951, 0.019437391, 0.013324453, 0.03442656, 0.024869615, 0.055373646, 0.016404511, 0.05860872, -0.030760143, -0.03534316, 0.029007811, 0.031083649, -0.0042325524, 0.023804737, -0.032943815, -0.055751074, 0.04725901, 0.021095365, 0.0029132497, 0.0040809084, 0.048876546, -0.0042797304, 0.026473671, 0.031919375, -0.0047144433, 0.0007182028, -0.000442295, 0.038713027, 0.053675234, 0.0026217562, 0.04515621, 0.00048104845, -0.038200807, 0.00911212, -0.0075215423, 0.029466113, -0.058177378, 0.012576343, -0.04922701, 0.0063555683, 0.03434568, -0.009307572, 0.00050168886, -0.018089443, 0.0042561414, 0.039548755, 0.013445768, 0.009968066, -0.032916855, 0.03585538, -0.060010586, -0.019396951, 0.04397002, 0.0046335664, -0.04248728, 0.034264803, -0.035774503, 0.008532502, 0.0068138703, 0.0060286913, -0.0074878437, -0.013425549, 0.009563682, -0.016633661, 0.0014094467, 0.017388511, 0.04216377, -0.008768393, 0.06502494, 0.02275334, 0.029169565, 0.030517511, -0.023818217, -0.08810179, 0.036044095, -0.026770221, -0.008283132, 0.00042144395, 0.016337113, 0.001023597, -0.038524315, -0.024559587, -0.020650543, 0.02273986, 0.030220963, -0.03361779, 0.024074327, -0.02779466, 0.03793122, 0.021850215, 0.028199043, -0.030975813, 0.037715547, -0.016498867, -0.04316125, 0.026864577, 0.042864703, -0.014328673, 0.0022392764, 0.040276647, -0.0013934398, 0.035289243, 0.012481986, 0.0054423343, 0.013695138, -0.037985135, 0.057206854, 0.05060192, 0.05305518, 0.035208367, -0.013412069, 0.0041280864, 0.016269716, -0.00505143, -0.017536785, -0.022537667, -0.0048660873, 0.012542644, 0.031245403, -0.043403883, 0.031407155, -0.006928446, -0.028792141, -0.0038921959, -0.029223483, -0.059471406, -0.022537667, -0.032728143, 0.046638954, 0.0032451816, -0.048633914, -0.0034777024, 0.028846057, -0.0008871174, -0.01970698, 0.00682398, 0.03251247, -0.008626859, -0.03253943, 0.035316203, 0.01567662, -0.02349471, 0.047420762, -0.07489192, -0.011262095, 0.03156891, -0.012596562, 0.0217289, 0.027322879, -0.023845175, 0.018749937, 0.009489545, 0.016027085, 0.030167045, 0.027228521, -0.036205847, 0.041381963, -0.026446713, 0.0449675, 0.011531684, 0.013674919, 0.020300075, -0.002677359, 0.010931848, 0.03235072, 0.052489042, -0.004657156, 0.036017135, -0.012246096, -0.03189242, 0.00021777763, -0.0019309336, 0.012178698, 0.02273986, -0.003942744, -0.022065887, -0.00405058, 0.009496284, 0.043269087, -0.049173094, 0.025489671, 0.03895566, 0.049388766, -0.0055535403, 0.006011842, 0.006618418, 0.049954902, -0.022564627, 0.010419628, 0.03005921, -0.028199043, 0.048606955, 0.01035897, -0.009684997, -0.04623457, -0.013439028, -0.053216934, -0.041355003, -0.009752394, -0.029250441, -0.019396951, 0.011599081, -0.011686698, 0.008997544, -0.0535674, 0.018305115, -0.002480222, -0.026905015, -0.0022645504, 0.028711263, 0.050925422, -0.051356766, 0.010298313, -0.012947028, 0.006898117, -0.0018534266, 0.007818091, 0.027430713, -0.025327917, 0.014503906, 0.012259575, -0.0025425644, 0.034803983, -0.052354246, -0.02124364, 0.009968066, 0.011531684, 0.04874175, 0.027471153, -0.025179643, -0.029843539, -8.0508216e-05, -0.008006803, 0.02199849, 0.03235072, -0.033159487, -0.020758377, 0.018049005, 0.00057708967, -0.03563971, 0.037850343, 0.031245403, -0.021944571, -0.02779466, 0.036798943, 0.0077843918, 0.012704397, -0.019140841, 0.02248375, -0.0145443445, 0.023090325, 0.0012746521, -0.07413706, -0.007939406, -0.025624465, -0.008013543, -0.052974302, -0.033267323, 0.0048660873, -0.021270597, 0.009058202, 0.012704397, -0.04364651, -0.00075737754, 0.02554359, 0.034723107, -0.027511591, 0.045129254, 0.009334531, -0.0041381964, -0.022726381, 0.022146763, -0.029034771, -0.020138323, -0.0043403883, 0.0054490743, -0.0048660873, -0.025503151, 0.01694369, 0.031164527, 0.07979844, -0.0034911819, -0.003511401, -0.0030514142, 0.052354246, 0.0039023056, 0.0147734955, 0.005988253, 0.0069351853, -0.026878055, -0.016525825, -0.004960444, 0.038713027, 0.00030792155, -0.013674919, 0.032000255, 0.017563745, -0.05860872, 0.03946788, -0.053001262, -0.050898466, 0.00657124, -0.018022047, -0.013223357, -0.034992695, -0.011531684, -0.03189242, -0.013634481, -0.0016866182, -0.0073058708, 0.014706098, -0.026851097, 0.002525715, -0.01594621, -0.0073867477, 0.01566314, 0.002476852, -0.009381709, -0.026096247, -0.06491711, -0.05108718, -0.0151643995, -0.0152857145, 0.0037708806, -0.0043808264, 0.0035686886, 0.059255734, -0.015029605, -0.032755103, -0.02629844, -0.0037001136, 0.0016883032, 0.07144117, 0.0058332393, -0.004316799, -0.038793903, -0.023130765, -0.008390968, 0.037688587, -0.013708618, -0.020286597, -0.01843991, -0.021297557, -0.044617034, 0.006422966, -0.046126734, -0.034507435, -0.047339886, 0.009159298, 0.057961706, -0.04618065, -0.030814061, -0.028630387, -0.0022948792, 0.039279167, 0.015730537, -0.054214414, 0.042055935, -0.019639583, 0.008377489, 0.0054760333, 0.030167045, -0.02096057, -0.00081340154, -0.021931091, 0.028846057, 0.030032251, 0.04192114, -0.028819099, 0.010015244, 0.031649787, 0.00039743364, -0.009037982, -0.00011899841, -0.012616781, 0.062329054, -0.04553364, 0.04065407, -0.0019343034, 0.007858529, 0.01643147, 0.05103326, 0.03814689, 0.0145982625, -0.0012797068, 0.036852863, -0.020407911, 0.018305115, -0.030301841, -0.016876291, -0.011808013, 0.036502395, -0.020610103, 0.017792895, 0.013465987, -0.0017573854, 0.0328899, 0.031029731, -0.028360797, 0.041543715, -0.031191485, 0.0207449, -0.03410305, 0.004192114, 0.018520787, -0.10239003, 0.042595115, 0.0077776522, 0.009516504, -0.049981862, -0.056721594, -0.0657798, -0.006871158, -0.03658327, 0.011565383, -0.039333083, -0.015029605, -0.011471026, -0.016606703, -0.012758315, 0.009240174, -0.059147898, 0.02400693, 0.026365837, -0.038039055, 0.00089806947, -0.015083523, -0.00783157, 0.04771731, -0.05383699, -0.01845339, -0.033240363, 0.0062544723, -0.012057383, -0.0146656595, -0.034777023, 0.057422526, -0.002227482, 0.028846057, 0.0691227, 0.03946788, 0.021297557, 0.0155418245, 0.016229277, 0.038012095, -0.002679044, -0.039360043, -0.012468507, 0.01490829, 0.0077843918, 0.05860872, 0.020637063, -0.008195516, -0.048175614, -0.0718186, -0.0066554863, -0.0074878437, -0.026473671, -0.033995215, -0.0149756875, -0.041004535, 0.053513482, 0.023589065, 0.0025375094, 0.039818343, -0.053863946, 0.036151927, -0.059471406, -0.05275863, -0.032485515, 0.037499875, 0.003184524, 0.016229277, 0.022443311, -0.0136547, -0.045075335, -0.026433233, 0.005432225, -0.04822953, 0.010325272, -0.03989922, 0.004222443, -0.008896448, -0.0038787164, 0.04922701, -0.024707861, 0.093924925, -0.008053981, 0.04547972, 0.017159361, -0.048310407, -0.021189721, -0.0150970025, 0.024734821, -0.017590703, -0.01567662, -0.010096121, 0.028387757, 0.012839192, -0.009765874, -0.009287353, -0.009246914, -0.06691207, -0.05526581, 0.0019646322, -0.030355759, -0.0021179612, -0.026675863, -0.016350593, 0.00083319953, -0.0049874024, -0.008040502, 0.079744525, 0.0038753466, 0.0055468003, -0.04213681, 0.054214414, 0.040276647, 0.026244521, -0.032000255, 0.017294155, 0.008754914, -0.037526835, 0.0073395693, -0.06302998, -0.057638198, 0.04143588, 0.023211641, -0.031191485, 0.01947783, -0.01641799, -0.03361779, -0.01796813, 0.016781935, 0.034507435, 0.002859332, 0.05122197, -0.015757496, -0.042028975, 0.0030396197, -0.054510962, -0.024573067, -0.025327917, 0.050359286, 0.032728143, -0.032108087, 0.025961453, 0.07322046, -0.066534646, -0.023225121, 0.019531747, 0.023346435, 0.0072452133, 0.016539305, 0.0149487285, -0.009570421, -0.033644747, -0.0033429076, 0.0010640353, -0.00833031, 0.010520724, 0.01947783, 0.03968355, -0.01641799, -0.062544726, 0.0023083587, 0.01821076, -0.026905015, -0.053486522, 0.028873017, -0.028819099, 0.027255481, 0.07219602, 0.0009494599, 0.0012822342, 0.018359033, -0.00028412187, 0.02349471, -0.010709436, 0.0071306378, 0.03944092, -0.054942306, -0.021607585, 0.024222601, 0.024896575, -0.008303352, -0.049820106, -0.04777123, 0.009011024, 0.022106325, 0.03240464, 0.01012308, 0.008451626, -0.040034015, 0.0075485012, -0.0016765086, -0.023292517, -0.0011845081, 0.0070362813, 0.026500631, 0.046854626, -0.07025498, 0.050386246, 0.015231797, -0.018817335, -0.0017843443, -0.031973295, 0.027174605, -0.041031495, -0.07187252, -0.008916667, -0.0054524443, -0.052138574, 0.0470703, -0.024330437, -0.02453263, -0.031272363, 0.007885488, -0.022321997, 0.001655447, 0.032108087, -0.011767575, 0.010008504, 0.017038045, -0.018075963, 0.011760835, 0.003385031, -0.039494835, 0.06491711, -0.011221656, 0.022173721, 0.0074743642, 0.017846813, -0.00052780536, -0.015757496, -0.01340533, -0.0076091588, -0.034642227, -0.027740741, 0.02601537, -0.033213407, 0.035990175, 0.01036571, -0.0073058708, -0.05526581, 0.016013606, -0.049415722, 0.0014852687, -0.012468507, 0.008390968, 0.019167801, -0.041786347, 0.031946335, -0.010183737, 0.045452762, -0.0139916865, -0.02024616, 0.04351172, -0.03458831, 0.01872298, -0.0008529975, 0.016566264, -0.06772084, -0.0039865524, -0.012872891, 0.02755203, 0.07677904, 0.034803983, 0.018035525, 0.0062780613, -0.044940542, -0.025327917, -0.051356766, -0.05313606, 0.043781307, -0.012043904, 0.00454595, 0.013351412, -0.012704397, -0.028630387, 0.0007506378, -0.0155418245, -0.0055232113, -0.011012724, -0.0007523227, -0.009503024, 0.04192114, -0.029412195, 0.04820257, 0.074352734, -0.010419628, -0.04116629, 0.05052104, 0.038982615, -0.04698942, -0.016377551, -0.040546235, 0.026635425, -0.01364122, 0.00057751086, -0.030436635, 0.013465987, -0.010830752, 0.03655631, -0.020300075, 0.019262157, -0.042783827, 0.03283598, 0.012340452, -0.020421391, -0.04270295, 0.022726381, 0.0070901993, -0.0024094547, 0.0150700435, 0.028738223, 0.032620307, -0.041301087, -0.0007270487, -0.009408668, 0.0025947972, 0.067613006, 0.062167298, 0.056452006, 0.0019646322, 0.034130007, 0.030005293, 0.021351475, -0.0148408925, 0.015258756, -0.036852863, 0.001278022, 0.046881583, 0.030490553, -0.010507244, -0.0071643363, -0.012805493, -0.037041575, -0.02226808, -0.021594105, -0.0359093, 0.030140087, 0.012967247, 0.0434578, -0.016889771, 0.017469388, -0.005007622, -0.0028778662, -0.0012064122, -0.032135047, 0.05052104, 0.009786093, 0.01567662, 0.005112088, -0.0068374593, -0.012246096, -0.025422273, 0.029843539, -0.0207449, -0.02147279, -0.00506154, 0.027471153, -0.027363317, 0.03453439, -0.025880575, 0.040249687, 0.017118922, -0.057961706, 0.03563971, -0.041759387, 0.03507357, -0.023090325, 0.009422147, 0.0007169391, -0.0074945833, -0.05936357, 0.008741434, -0.0016335428, 0.0069351853, 0.0016032141, 0.036852863, 0.004973923, 0.005664746, 0.04618065, -0.05022449, -0.01389733, -0.01010286, -0.009240174, 0.0065543903, 0.029331319, -0.10114992, -0.012407849, -0.0090919, 0.023737341, -0.0145982625, 0.048984382, 0.035208367, -0.068313934, -0.012872891, 0.04016881, -0.0038955659, 0.03714941, 0.039063495, -0.0023942902, -0.001529077, -0.016027085, -0.0071171583, -0.026972413, 0.053378686, -0.0657798, 0.0017759197, 0.01997657, -0.065564126, -0.045398843, -0.028360797, 0.0009241859, -0.018601663, -0.0470703, -0.040707987, 0.0043100594, -0.005075019, 0.0052502523, 0.030247923, 0.020610103, -0.020151801, 0.0023589067, 0.0013386795, -0.017536785, 0.043269087, 0.03838952, 0.0040809084, -0.03504661, 0.03607105, -0.030463593, -0.011282314, 0.020879693, 0.009684997, 0.04251424, 0.013270535, -0.03712245, 0.0037809904, 0.03639456, -0.0008281447, -0.03391434, -0.00056192523, -0.008633598, -0.053944826, -0.04849912, -0.0152048385, 0.016593223, -0.03844344, -0.012347192, -0.0145173855, -0.073543966, 0.21017183, 0.03995314, -0.0022038927, 0.050844546, 0.037203327, 0.069661885, -0.04162459, -0.0075754602, 0.00010441321, -0.03944092, 0.0024313587, -0.021445831, 0.03340212, -0.022712901, 0.00019629473, 0.031191485, -0.019855253, 0.0007118843, -0.005489513, -0.01694369, -0.008930147, 0.022618545, 0.012980727, 0.07289696, 0.010338751, -0.03202721, -0.015838373, -0.052138574, -0.024316957, -0.037419, 0.071656846, -0.030598389, 0.02679718, -0.026635425, -0.055697154, 0.033240363, 0.0028020442, -0.019491307, -0.002652085, 0.023117285, 0.025449233, 0.038012095, 0.05227337, -0.027498111, 0.027242001, 0.009813052, -0.029708743, 0.060657598, 0.0149756875, -0.033429075, 0.03706853, -0.029708743, 0.032916855, 0.0059747733, -0.008269653, 0.014059084, -0.017900731, -0.029520031, 0.024384355, 0.0015442413, 0.008357269, 0.021432351, -0.03838952, -0.0154609475, -0.020327035, 0.0153396325, 0.036017135, 0.028495591, -0.0149622075, 0.012576343, -0.019289115, -0.018480347, -0.020421391, -0.06033409, 0.0016613442, 0.0036091271, -0.01490829, 0.012731356, 0.062221218, -0.025705343, -0.03132628, -0.02299597, 0.004653786, -0.0031457704, -0.004599868, -0.02023268, -0.045344926, 0.009631079, -0.024680903, 0.03337516, 0.036852863, 0.0018837554, 0.0044347444, 0.03760771, 0.011969767};
//		
//		Float [] vector = new Float[qvector.length];
//		for(int dblIndex = 0;dblIndex < qvector.length;dblIndex++)
//			vector[dblIndex] = new Double(qvector[dblIndex]).floatValue();
//		
//		Field content = Field.builder().name("content").build();
//		Field source = Field.builder().name("source").build();
//		Field divider = Field.builder().name("divider").build();
//		Field part = Field.builder().name("part").build();
//		Field cert = Field.builder().name("certainty").build();
//		
//		Field _additional = Field.builder()
//			      .name("_additional")
//			      .fields(new Field[]{
//			        Field.builder().name("certainty").build(),  // only supported if distance==cosine
//			        Field.builder().name("distance").build()   // always supported
//			      }).build();
//		NearVectorArgument nearVector = NearVectorArgument.builder().vector(vector).build();
//		GraphQLResponse response = w.client.graphQL().get().withClassName("Vector_Table").withFields(content, source, divider, part, _additional).withNearVector(nearVector).withAutocut(1).withLimit(3).run().getResult();
//		
//		ArrayList outputs = ((ArrayList)(((LinkedTreeMap)((LinkedTreeMap)response.getData()).get("Get")).get("Vector_Table")));
//		// each of the output is another treemap with each of the fields.. yay 
//		LinkedTreeMap additional = (LinkedTreeMap)outputs.get(0);
//				
//		System.err.println("response " + outputs);
//		
//		// deletes it
//		
//		//w.client.schema().classDeleter()
//	    //  .withClassName("Vector_Table")
//	    //  .run();
//
//		GraphQLResponse resp = w.client.graphQL().get().withClassName("Vector_Table").withFields(content).run().getResult();
//		
//		System.err.println("Response.. from query " + resp);
//	}

}
