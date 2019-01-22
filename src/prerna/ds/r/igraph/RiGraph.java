package prerna.ds.r.igraph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;

import prerna.cache.CachePropFileFrameObject;
import prerna.cache.ICache;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.test.TestUtilityMethods;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RiGraph extends AbstractTableDataFrame {

	public static final String DATA_MAKER_NAME = "RiGraph";
	
	private String graphName = "";
	private transient AbstractRJavaTranslator rJavaTranslator;

	public RiGraph() {
		// create a random var name
		this(Utility.getRandomString(8));
	}
	
	public RiGraph(String varName) {
		this.graphName = varName;
	}
	
	public RiGraph(String varName, AbstractRJavaTranslator rJavaTranslator) {
		this.graphName = varName;
		setRJavaTranslator(rJavaTranslator);
	}
	
	public void setRJavaTranslator(AbstractRJavaTranslator rJavaTranslator) {
		this.rJavaTranslator = rJavaTranslator;
		this.rJavaTranslator.startR();
		this.rJavaTranslator.executeR("library(igraph)");
		this.rJavaTranslator.executeR(this.graphName + "<- make_empty_graph()");
	}
	
	@Override
	public boolean isEmpty() {
		return isEmpty(this.graphName);
	}
	
	private boolean isEmpty(String script) {
		int size = rJavaTranslator.getInt("length(as_ids(" + script + "))");
		if(size == 0) {
			return true;
		}
		return false;
	}

	private void addRelationship(Iterator<IHeadersDataRow> it, Map<Integer, Set<Integer>> cardinality) {
		boolean hasRel = false;

		// we want to add everything in one go
		// so we will make a file with the script
		// and execute a single command to execute
		FileWriter writer = null;
		BufferedWriter bufferedWriter = null;

		String insightCacheDir = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
		final String FILE_SEPARATOR = System.getProperty("file.separator");
		String csvCache = DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		String path = insightCacheDir + FILE_SEPARATOR + csvCache + FILE_SEPARATOR + Utility.getRandomString(10) + ".r";

		boolean isError = false;
		try {
			writer = new FileWriter(path);
			bufferedWriter = new BufferedWriter(writer);
		} catch (IOException ex) {
			isError = true;
			throw new IllegalArgumentException("Unable to write to file to import igraph");
		} finally {
			if(isError) {
				cleanUpWriters(writer, bufferedWriter);
			}
		}

		try {
			while(it.hasNext()) {
				StringBuilder rScriptBuilder = new StringBuilder();

				IHeadersDataRow nextRow = it.next();
				String[] headers = nextRow.getHeaders();
				Object[] values = nextRow.getValues();
				for(Integer startIndex : cardinality.keySet()) {
					Set<Integer> endIndices = cardinality.get(startIndex);
					if(endIndices==null) continue;

					for(Integer endIndex : endIndices) {
						hasRel = true;

						//get from vertex
						String startNode = headers[startIndex];
						Object startNodeValue = values[startIndex];
						String startUniqueId = startNode + ":" + startNodeValue;
						rScriptBuilder.append(upsertVertexSyntax(startUniqueId, startNode, startNodeValue));

						//get to vertex	
						String endNode = headers[endIndex];
						Object endNodeValue = values[endIndex];
						String endUniqueId = endNode + ":" + endNodeValue;
						rScriptBuilder.append(upsertVertexSyntax(endUniqueId, endNode, endNodeValue));

						rScriptBuilder.append(upsertEdgeSyntax(startUniqueId, endUniqueId));
					}
				}

				// this is to replace the addRow method which needs to be called on the first iteration
				// since edges do not exist yet
				if(!hasRel) {
					String node = headers[0];
					Object nodeValue = values[0];
					String nodeId = node + ":" + nodeValue;
					rScriptBuilder.append(upsertVertexSyntax(nodeId, node, nodeValue));
				}

				bufferedWriter.write(rScriptBuilder.toString());
				bufferedWriter.write("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			cleanUpWriters(writer, bufferedWriter);
		}

		// execute the script which has all the insertions
		String script = "source(\"" + path.replace("\\", "/") + "\")";
		this.rJavaTranslator.executeR(script);
		ICache.deleteFile(new File(path));
	}
	
	/**
	 * Adding this cause this seems to be used a lot
	 * @param writer
	 * @param bufferedWriter
	 */
	private void cleanUpWriters(FileWriter writer, BufferedWriter bufferedWriter) {
		try {
			if(bufferedWriter != null) {
				bufferedWriter.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			if(writer != null) {
				writer.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void addRelationship(String[] headers, Object[] values, Map<Integer, Set<Integer>> cardinality) {
		boolean hasRel = false;
		
		// we want to add everything in one go
		StringBuilder rScriptBuilder = new StringBuilder();
		for(Integer startIndex : cardinality.keySet()) {
			Set<Integer> endIndices = cardinality.get(startIndex);
			if(endIndices==null) continue;
			
			for(Integer endIndex : endIndices) {
				hasRel = true;
				
				// get from vertex
				String startNode = headers[startIndex];
				Object startNodeValue = values[startIndex];
				String startUniqueId = startNode + ":" + startNodeValue;
				rScriptBuilder.append(upsertVertexSyntax(startUniqueId, startNode, startNodeValue));
				
				// get to vertex	
				String endNode = headers[endIndex];
				Object endNodeValue = values[endIndex];
				String endUniqueId = endNode + ":" + endNodeValue;
				rScriptBuilder.append(upsertVertexSyntax(endUniqueId, endNode, endNodeValue));
				
				// add the edge between the nodes
				rScriptBuilder.append(upsertEdgeSyntax(startUniqueId, endUniqueId));
			}
		}
		
		// if we have a relationship, execute the r script
		// else we just need to insert a single node
		if(hasRel) {
			this.rJavaTranslator.executeR(rScriptBuilder.toString());
		} else {
			String node = headers[0];
			Object nodeValue = values[0];
			String nodeId = node + ":" + nodeValue;
			this.rJavaTranslator.executeR(upsertVertexSyntax(nodeId, node, nodeValue));
		}
	}
	
	/**
	 * Get the conditional string syntax to insert a vertex
	 * @param uniqueId
	 * @param nodeType
	 * @param nodeValue
	 * @return
	 */
	private String upsertVertexSyntax(String uniqueId, String nodeType, Object nodeValue) {
		String addScript = this.graphName + " <- add_vertices(" + this.graphName + ", 1, "
				+ "name=\"" + uniqueId + "\", "
				+ "value=\"" + nodeValue + "\", "
				+ "type=\"" + nodeType + "\")";
		
		// script includes an if statement so we do not add the same vertex multiple times
		return "if(length(as_ids(V(" + this.graphName + ")[vertex_attr(" + this.graphName + ", \"name\") == \"" + uniqueId+ "\"])) == 0) "
				+ "{" + addScript + "};";
	}
	
	/**
	 * Get the conditional string syntax to add an edge
	 * @param fromVertex
	 * @param toVertex
	 * @return
	 */
	private String upsertEdgeSyntax(String fromVertex, String toVertex) {
		String uniqueId = fromVertex + ":" + toVertex;
		String addScript = this.graphName + " <- add_edges(" + this.graphName + ", "
				+ "c(\"" + fromVertex + "\", \"" + toVertex + "\"), "
				+ "name=\"" + uniqueId + "\")";
		
		// script includes an if statement so we do not add the same edge multiple times
		return "if(length(as_ids(E(" + this.graphName + ")[edge_attr(" + this.graphName + ", \"name\") == \"" + uniqueId+ "\"])) == 0)"
				+ "{" + addScript + "};";
	}

	@Override
	public void removeColumn(String columnHeader) {
		String deleteColScript = this.graphName + " <- delete_vertices(" + this.graphName + ", V(" + this.graphName + ")[vertex_attr("
			+ this.graphName + ", \"type\") == \"" + columnHeader + "\"])";
		this.rJavaTranslator.executeR(deleteColScript);
	}
	
	@Override
	public long size(String tableName) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public String getDataMakerName() {
		return DATA_MAKER_NAME;
	}
	
	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadDIHelper();
		String engineProp = "C:\\workspace2\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId(Constants.LOCAL_MASTER_DB_NAME);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);
		
		engineProp = "C:\\workspace2\\Semoss_Dev\\db\\Movie_RDBMS.smss";
		coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("Movie_RDBMS");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("Movie_RDBMS", coreEngine);
		
		Iterator<IHeadersDataRow> it = WrapperManager.getInstance().getRawWrapper(coreEngine, "Select Title, Movie_Budget from Title");
		
		
		Insight in = new Insight();
		AbstractRJavaTranslator translator = RJavaTranslatorFactory.getRJavaTranslator(in, LogManager.getLogger(RiGraph.class.getName()));
		RiGraph g = new RiGraph("g", translator);
		
		Map<Integer, Set<Integer>> cardinality = new Hashtable<Integer, Set<Integer>>();
		Set<Integer> s = new HashSet<Integer>();
		s.add(1);
		cardinality.put(0, s);
		
		long start = System.currentTimeMillis();
		g.addRelationship(it, cardinality);
		long end = System.currentTimeMillis();
		System.out.println("Time to insert = " + (end-start) + "ms");
		
		Object o = translator.executeR("V(g)");
		System.out.println(o);
		o = translator.executeR("E(g)");
		System.out.println(o);
	}
	 
	@Override
	public Double getMax(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getMin(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IRawSelectWrapper query(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IRawSelectWrapper query(SelectQueryStruct qs) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public CachePropFileFrameObject save(String fileName) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void open(CachePropFileFrameObject cf) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String uniqueHeaderName, List<String> attributeUniqueHeaderName) {
		// TODO Auto-generated method stub
		return null;
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Deprecated DataMakerComponent stuff
	 */	
	
	@Override
	@Deprecated
	public void processDataMakerComponent(DataMakerComponent component) {
		// TODO Auto-generated method stub
	}
	
	@Override
	@Deprecated
	public void addRow(Object[] cleanCells, String[] headers) {
		// TODO Auto-generated method stub
	}

}
