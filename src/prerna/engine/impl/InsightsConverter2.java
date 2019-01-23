package prerna.engine.impl;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.om.OldInsight;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.PKQLTransformation;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class InsightsConverter2 {

	private static final Logger LOGGER = LogManager.getLogger(InsightsConverter2.class.getName());

	private IEngine coreEngine;
	private ENGINE_TYPE engineType;
	// insights will always be RDBMS
	private IEngine insightsEngine;

	public InsightsConverter2(IEngine coreEngine) {
		this.coreEngine = coreEngine;
		this.engineType = coreEngine.getEngineType();
		this.insightsEngine = coreEngine.getInsightDatabase();
	}

	public void modifyInsightsDatabase() throws IOException {
		LOGGER.info("STARTING PROCEDURE");
		long start = System.currentTimeMillis();
		modifyTableStructure();
		modifyExistingInsights();
		this.insightsEngine.commit();
		long end = System.currentTimeMillis();
		LOGGER.info("END PROCEDURE >>> TIME = " + (end-start) + " ms");
	}

	/**
	 * See if the necessary columns are already in the table
	 * If not, add them into the table
	 * @throws IOException 
	 */
	private void modifyTableStructure() throws IOException {
		// before we do anything
		// make sure the QUESTION_ID Table exists!

		String query = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'QUESTION_ID'";

		Map<String, Object> mapRet = (Map<String, Object>) this.insightsEngine.execQuery(query);
		Statement stat = (Statement) mapRet.get(RDBMSNativeEngine.STATEMENT_OBJECT);
		ResultSet rs = (ResultSet) mapRet.get(RDBMSNativeEngine.RESULTSET_OBJECT);
		try {
			if (!rs.next()) {
				throw new IOException("COULD NOT FIND INSIGHTS QUESTION_ID TABLE FOR ENGINE = " + this.coreEngine.getEngineId());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(stat != null) {
				try {
					stat.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		// This routine will append the QUESTION_PKQL column
		String q1 = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='QUESTION_ID' and COLUMN_NAME='QUESTION_PKQL'";
		IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(this.insightsEngine, q1);
		boolean hasCol = false;
		if(manager.hasNext()) {
			manager.next();
			hasCol = true;
		}

		if(!hasCol) {
			// need to alter the table
			String alterQuery = RdbmsQueryBuilder.makeAlter("QUESTION_ID", new String[]{"QUESTION_PKQL"}, new String[]{"ARRAY"});
			LOGGER.info("ALTERING TABLE: " + alterQuery);
			try {
				insightsEngine.insertData(alterQuery);
			} catch (Exception e) {
				e.printStackTrace();
			}
			LOGGER.info("DONE ALTER TABLE");
		}
	}

	/**
	 * Run through all the existing insights
	 * If they are PKQL, grab the statements and add them in the new QUESTION_PKQL column
	 * @throws IOException 
	 */
	private void modifyExistingInsights() throws IOException {
		String query = "SELECT ID, QUESTION_MAKEUP, QUESTION_LAYOUT FROM QUESTION_ID";
		IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(this.insightsEngine, query);
		while(manager.hasNext()) {
			IHeadersDataRow row = manager.next();
			Object[] values = row.getValues();
			String id = (String) values[0];
			Clob clob = (Clob) values[1];
			String layout = (String) values[2];
			
			if(clob != null) {
				InputStream insightDefinition = null;
				try {
					insightDefinition = clob.getAsciiStream();
				} catch (SQLException e) {
					e.printStackTrace();
					continue;
				}
				
				OldInsight oldInsight = new OldInsight();
				oldInsight.setMakeup(insightDefinition);
				List<DataMakerComponent> dmcList = oldInsight.getDataMakerComponents();
				processInsight(id, layout, dmcList);
			} else {
				LOGGER.info("Insight " + id + " is not saved using DMC format and is already converted!");
			}
		}
	}

	/**
	 * This is what actually processes the dmcs to get the pkql statements out
	 * @param id
	 * @param layout 
	 * @param dmcList
	 */
	private void processInsight(String id, String layout, List<DataMakerComponent> dmcList) {
		LOGGER.info("STARTING TO PROCESS ID = " + id );

		/*
		 * If this is pkql (or pixel)
		 * Then there can only be 1 dmc
		 * And every transformation must be a PKQLTransformation
		 * And every one needs to be a PostTransformation
		 */

		if(dmcList.size() == 1) {
			DataMakerComponent dmc = dmcList.get(0);
			if(dmc.getPreTrans().size() > 0) {
				LOGGER.info("ID = " + id + " IS NOT VALID PKQL INSIGHT");
				// cannot have pre trans
				return;
			}
			
			String engineName = dmc.getEngineName();
			String query = dmc.getQuery();
			
			List<String> pixelStrings = new Vector<String>();
			
			/*
			 * Two cases
			 * Case 1:
			 * We have just a query and no transformations
			 * .. we can just make the data.import statement
			 * 
			 * Case 2:
			 * Engine is local master and the query is useless so we
			 * just combine all the pkql transformation expressions
			 * 
			 */
			
			// test for case 1
			if(engineName.equals(Constants.LOCAL_MASTER_DB_NAME)) {
				// grab all the post trans
				List<ISEMOSSTransformation> transList = dmc.getPostTrans();
				int size = transList.size();
				for(int i = 0; i < size; i++) {
					String expression = transList.get(i).getProperties().get(PKQLTransformation.EXPRESSION) + "";
					pixelStrings.add(expression);
				}
				
				// cool, we have the strings
				// just save it now
				
				StringBuilder updateQ = new StringBuilder("UPDATE QUESTION_ID SET QUESTION_PKQL=(");
				int numPixels = pixelStrings.size();
				for(int i = 0; i < numPixels; i++) {
					updateQ.append("'").append(pixelStrings.get(i).replace("'", "''")).append("'");
					if(i+1 != numPixels) {
						updateQ.append(",");
					}
				}
				updateQ.append(") WHERE ID=").append(id);
				try {
					this.insightsEngine.insertData(updateQ.toString());
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
			
			// test for case 2
			// damn, we put a lot of crappy stuff here
			else if(dmc.getPostTrans().size() == 0 && 
					query != null && 
					!query.isEmpty() && 
					!query.trim().toUpperCase().startsWith("CONSTRUCT") &&
					!query.equals("@@") && 
					!query.trim().toUpperCase().startsWith("NULL") && 
					!query.trim().toUpperCase().startsWith("NONE") && 
					!layout.startsWith("prerna.ui.components.") && 
					!layout.startsWith("perna.grayedout")
					){
				// we can create a data.import statement
				
				Set<String> returnVariables = null;
				
				if( (engineType == ENGINE_TYPE.JENA || engineType == ENGINE_TYPE.SESAME)) {
					// let us try to see if we should convert any data types to numbers
					try {
						SPARQLParser parser = new SPARQLParser();
						ParsedQuery parsedQuery = parser.parseQuery(query, null);
						returnVariables = parsedQuery.getTupleExpr().getBindingNames(); 
					} catch (Exception e) {
						LOGGER.info("ID = " + id + " IS NOT VALID PKQL INSIGHT");
						// TODO: for right now, update the field to be null since i'm updating the logic
						String updateQ = "UPDATE QUESTION_ID SET QUESTION_PKQL=null WHERE ID=" + id;
						try {
							this.insightsEngine.insertData(updateQ);
						} catch (Exception e1) {
							e1.printStackTrace();
						}
						return;
					}
					
					// add this for sparql queries
					// START 1 - SETTING UP THE INSIGHT
					pixelStrings.add("AddPanel(0);");
					pixelStrings.add("Panel(0)|AddPanelEvents({\"onSingleClick\":{\"Unfilter\":[{\"panel\":\"\",\"query\""
							+ ":\"<encode>UnfilterFrame(<Frame>__<SelectedColumn>);</encode>\",\"options\":{},\"refresh\":true}]},"
							+ "\"onBrush\":{\"Filter\":[{\"panel\":\"\",\"query\":\"<encode>if(IsEmpty(<SelectedValues>), "
							+ "UnfilterFrame(<Frame>__<SelectedColumn>), SetFrameFilter(<Frame>__<SelectedColumn>==<SelectedValues>));</encode>\","
							+ "\"options\":{},\"refresh\":true}]}});");
					pixelStrings.add("Panel(0)|RetrievePanelEvents();");
					pixelStrings.add("CreateFrame(grid).as(['FRAME']);");
					// END START 1
					// START 2 - IMPORTING THE DATA AND SET VIEW TO VISUALIZATION
					pixelStrings.add("Database(" + engineName + ") | Query(\"" + query.replace("\"", "'") + "\") | Import();");
					pixelStrings.add("Panel(0)|SetPanelView (\"visualization\");");
					// END START 2
					// START 3 - QUERYING THE DATA AND COLLECTING RESULTS
					StringBuilder sb = new StringBuilder("Frame() | Select(");
					// part a - get the initial results
					{
						int total = returnVariables.size();
						int counter = 0;
						for(String colName : returnVariables) {
							sb.append("f$").append(colName);
							if(counter + 1 != total) {
								sb.append(",");
							}
							counter++;
						}
					}
					if(layout.equals("WorldMap")) {
						layout = "Map";
					}
					sb.append(") | Format(type=['table']) | TaskOptions({\"0\": {\"layout\":\"" + layout + "\", \"alignment\" : {");
					
					/*
					 * This is super annoying
					 * but need to account for the correct labels
					 * for each of the visualizations
					 * 
					 * old playsheets used to do this based on the index of the return
					 * so i need to have special logic for all the different ones to do this now
					 */
					
					if(layout.equals("Grid")) {
						sb.append("\"label\" : [");
						int counter = 0;
						int total = returnVariables.size();
						for(String colName : returnVariables) {
							sb.append("\"").append(colName).append("\"");
							if(counter + 1 != total) {
								sb.append(",");
							}
							counter++;
						}
						sb.append("]");
					} else if(layout.equals("Pie") || layout.equals("Radial") || layout.equals("Column") || layout.equals("Line") ) {
						int counter = 0;
						int total = returnVariables.size();
						boolean added = false;
						for(String colName : returnVariables) {
							if(counter == 0) {
								sb.append("\"label\" : [\"" + colName + "\"] , ");
							} else {
								if(!added) {
									sb.append("'value': [");
								}
								sb.append("\"").append(colName).append("\"");
								if(counter + 1 != total) {
									sb.append(",");
								}
							}
							counter++;
						}
						sb.append("]");
					} else if(layout.equals("Scatter")) {
						int counter = 0;
						int total = returnVariables.size();
						for(String colName : returnVariables) {
							if(counter == 0) {
								sb.append("\"label\" : [\"" + colName + "\"]");
							} else if(counter == 1) {
								sb.append("\"x\" : [\"" + colName + "\"]");
							} else if(counter == 2) {
								sb.append("\"y\" : [\"" + colName + "\"]");
							} else if(counter == 3) {
								sb.append("\"size\" : [\"" + colName + "\"]");
							} else if(counter == 4) {
								sb.append("\"color\" : [\"" + colName + "\"]");
							}
							if(counter + 1 != total) {
								sb.append(",");
							}
							counter++;
						}
					} else if(layout.equals("HeatMap")) {
						int counter = 0;
						int total = returnVariables.size();
						for(String colName : returnVariables) {
							if(counter == 0) {
								sb.append("\"x\" : [\"" + colName + "\"]");
							} else if(counter == 1) {
								sb.append("\"y\" : [\"" + colName + "\"]");
							} else if(counter == 2) {
								sb.append("\"heat\" : [\"" + colName + "\"]");
							}
							if(counter + 1 != total) {
								sb.append(",");
							}
							counter++;
						}
					} else if(layout.equals("Map")) {
						int counter = 0;
						int total = returnVariables.size();
						for(String colName : returnVariables) {
							if(counter == 0) {
								sb.append("\"label\" : [\"" + colName + "\"]");
							} else if(counter == 1) {
								sb.append("\"latitude\" : [\"" + colName + "\"]");
							} else if(counter == 2) {
								sb.append("\"longitude\" : [\"" + colName + "\"]");
							} else if(counter == 3) {
								sb.append("\"size\" : [\"" + colName + "\"]");
							} else if(counter == 4) {
								sb.append("\"color\" : [\"" + colName + "\"]");
							}
							if(counter + 1 != total) {
								sb.append(",");
							}
							counter++;
						}
					}
					
					sb.append("} } }) | Collect(500);");
					pixelStrings.add(sb.toString());
					// END START 3
				} else {
					return;
				}
				
				StringBuilder updateQ = new StringBuilder("UPDATE QUESTION_ID SET QUESTION_PKQL=(");
				int numPixels = pixelStrings.size();
				for(int i = 0; i < numPixels; i++) {
					updateQ.append("'").append(pixelStrings.get(i).replace("'", "''")).append("'");
					if(i+1 != numPixels) {
						updateQ.append(",");
					}
				}
				updateQ.append(") WHERE ID=").append(id);
				try {
					this.insightsEngine.insertData(updateQ.toString());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			// guess you cannot be converted
			else {
				LOGGER.info("ID = " + id + " IS NOT VALID PKQL INSIGHT");
				// TODO: for right now, update the field to be null since i'm updating the logic
				String updateQ = "UPDATE QUESTION_ID SET QUESTION_PKQL=null WHERE ID=" + id;
				try {
					this.insightsEngine.insertData(updateQ);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return;
			}
		} else {
			// can only have 1 dmc
			LOGGER.info("ID = " + id + " IS NOT VALID PKQL INSIGHT");
			// TODO: for right now, update the field to be null since i'm updating the logic
			String updateQ = "UPDATE QUESTION_ID SET QUESTION_PKQL=null WHERE ID=" + id;
			try {
				this.insightsEngine.insertData(updateQ);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return;
		}
	}
	
	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadDIHelper();
		
		String engineProp = "C:\\workspace2\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new BigDataEngine();
		coreEngine.setEngineId(Constants.LOCAL_MASTER_DB_NAME);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);
		
		engineProp = "C:\\workspace2\\Semoss_Dev\\db\\TAP_Core_Data.smss";
		coreEngine = new BigDataEngine();
		coreEngine.setEngineId("TAP_Core_Data");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("TAP_Core_Data", coreEngine);
		
		InsightsConverter2 converter = new InsightsConverter2(coreEngine);
		converter.modifyInsightsDatabase();
	}
}


///**
// * Class to parse through and get the aggregated values that need to be cast to doubles
// *
// */
//class FindAggregationVisitor extends QueryModelVisitorBase<Exception> {
//	
//	public Set<String> values = new HashSet<String>();
//
//	public Set<String> getValue(){
//		return values;
//	}
//	
//	@Override
//	public void meet(Avg node) {
//		values.add(node.getArg().getParentNode().getParentNode().getSignature().replace("ExtensionElem (", "").replace(")", ""));
//	}
//	@Override
//	public void meet(Max node) {
//		values.add(node.getArg().getParentNode().getParentNode().getSignature().replace("ExtensionElem (", "").replace(")", ""));
//	}
//	@Override
//	public void meet(Min node) {
//		values.add(node.getArg().getParentNode().getParentNode().getSignature().replace("ExtensionElem (", "").replace(")", ""));
//	}
//	@Override
//	public void meet(Sum node) {
//		values.add(node.getArg().getParentNode().getParentNode().getSignature().replace("ExtensionElem (", "").replace(")", ""));
//	}
//	@Override
//	public void meet(Count node) {
//		values.add(node.getArg().getParentNode().getParentNode().getSignature().replace("ExtensionElem (", "").replace(")", ""));
//	}
//}
