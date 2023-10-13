package prerna.reactor.imports;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.impl.util.MetadataUtility;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.joins.BasicRelationship;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class NativeImporter extends AbstractImporter {

	private static final Logger logger = LogManager.getLogger(NativeImporter.class);

	private static final String CLASS_NAME = NativeImporter.class.getName();
	
	private NativeFrame dataframe;
	private SelectQueryStruct qs;
	private Iterator<IHeadersDataRow> it;

	public NativeImporter(NativeFrame dataframe, SelectQueryStruct qs) {
		this.dataframe = dataframe;
		this.qs = qs;
	}
	
	public NativeImporter(NativeFrame dataframe, SelectQueryStruct qs, Iterator<IHeadersDataRow> it) {
		this.dataframe = dataframe;
		this.qs = qs;
		this.it = it;
	}
	
	@Override
	public void insertData() {
		// see if we can parse the query into a valid qs object
		SemossDataType[] executedDataTypes = null;
		String[] columnNames = null;
		IDatabaseEngine database = this.qs.retrieveQueryStructEngine();
		if(this.qs.getQsType() == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY && database instanceof IRDBMSEngine) {
			IRDBMSEngine rdbms = (IRDBMSEngine) database;
			// if you are RDBMS
			// we will make a new QS
			// and we will wrap you
			String query = ((HardSelectQueryStruct) this.qs).getQuery();//.trim();
//			if(query.endsWith(";")) {
//				query = query.substring(0, query.length()-1);
//			}
//			if(this.it == null) {
				// use a prepared statement
				// so we dont have to actually do the execution of the query
				PreparedStatement ps = null;
				ResultSetMetaData rsMeta = null;
				try {
					ps = rdbms.getPreparedStatement(query);
					rsMeta = ps.getMetaData();
					int numCols = rsMeta.getColumnCount();
					columnNames = new String[numCols];
					executedDataTypes = new SemossDataType[numCols];
					for(int i = 0; i < numCols; i++) {
						columnNames[i] = rsMeta.getColumnName(i+1);
						executedDataTypes[i] = SemossDataType.convertStringToDataType(rsMeta.getColumnTypeName(i+1));
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				} finally {
					if(ps != null) {
						try {
							ps.close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
						if(rdbms.isConnectionPooling()) {
							try {
								ps.getConnection().close();
							} catch (SQLException e) {
								logger.error(Constants.STACKTRACE, e);
							}
						}
					}
				}
//				try {
//					StringBuilder newQueryB = new StringBuilder(" select * from (" + query + ") as customQuery ");
//					newQueryB = ((IRDBMSEngine) database).getQueryUtil().getFirstRow(newQueryB);
//					this.it = WrapperManager.getInstance().getRawWrapper(database, newQueryB.toString());
//				} catch (Exception e) {
//					logger.error(Constants.STACKTRACE, e);
//					// one more time
//					// try as well w/o changes - too bad on size...
//					try {
//						this.it = WrapperManager.getInstance().getRawWrapper(database, query);
//					} catch (Exception e1) {
//						logger.error(Constants.STACKTRACE, e1);
//						throw new SemossPixelException(
//								new NounMetadata("Error occurred executing query before loading into frame", 
//										PixelDataType.CONST_STRING, PixelOperationType.ERROR));
//					}
//				} finally {
//					if(this.it != null) {
//						((IRawSelectWrapper) this.it).cleanUp();
//					}
//				}
//			}
			
			String customFromAlias = "customquery";
			SelectQueryStruct newQs = new SelectQueryStruct();
			newQs.setEngineId(this.qs.getEngineId());
			newQs.setEngine(this.qs.getEngine());
			newQs.setCustomFrom(query);
			newQs.setCustomFromAliasName(customFromAlias);
			for(String p : columnNames) {
				QueryColumnSelector selector = new QueryColumnSelector();
				selector.setTable(customFromAlias);
				selector.setTableAlias(customFromAlias);
				selector.setColumn(p);
				String alias = p;
				while(alias.contains("__")) {
					alias = alias.replace("__", "_");
				}
				selector.setAlias(alias);
				newQs.addSelector(selector);
			}
			
			// swap the qs reference
			newQs.setBigDataEngine(this.qs.getBigDataEngine());
			newQs.setPragmap(this.qs.getPragmap());
			
			this.qs = newQs;
			this.qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
//			// lets see what happens
//			OpaqueSqlParser parser = new OpaqueSqlParser();
////			SqlParser parser = new SqlParser();
////			String query = ((HardSelectQueryStruct) this.qs).getQuery();
//			try {
//				SelectQueryStruct newQs = this.qs.getNewBaseQueryStruct();
//				newQs.merge(parser.processQuery(query));
//				// we were able to parse successfully
//				// override the reference
//				this.qs = newQs;
//				this.qs.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
//				
//				// we need to figure out the columns if there is a *
//				cleanUpSelectors(this.qs.getEngineId(), this.qs.getSelectors(), this.qs.getRelations());
//			} catch (Exception e) {
//				// we were not successful in parsing :/
//				e.printStackTrace();
//			}
		} else {
			if(it != null && (it instanceof IRawSelectWrapper)) {
				executedDataTypes = ((IRawSelectWrapper) it).getTypes();
			}
		}
		boolean ignore = true;
		QUERY_STRUCT_TYPE qsType = this.qs.getQsType();
		if(qsType == QUERY_STRUCT_TYPE.ENGINE 
				|| qsType == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY
				|| qsType == QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY
				) {
			ignore = MetadataUtility.ignoreConceptData(this.qs.getEngineId());
		} else if (qsType == QUERY_STRUCT_TYPE.FRAME && qs.getFrame().getFrameType() == DataFrameTypeEnum.NATIVE) {
			ignore = MetadataUtility.ignoreConceptData(((NativeFrame)qs.getFrame()).getEngineId());
			this.qs.setEngineId( ((NativeFrame)qs.getFrame()).getEngineId() );
			this.qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
			this.qs = QSAliasToPhysicalConverter.getPhysicalQs(this.qs, qs.getFrame().getMetaData());			
		}

		ImportUtility.parseNativeQueryStructIntoMeta(this.dataframe, this.qs, ignore, executedDataTypes);
		this.dataframe.mergeQueryStruct(this.qs);
	}

	@Override
	public void insertData(OwlTemporalEngineMeta metaData) {
		this.dataframe.setMetaData(metaData);
		this.dataframe.mergeQueryStruct(this.qs);
	}

	@Override
	public ITableDataFrame mergeData(List<Join> joins) throws Exception {
		QUERY_STRUCT_TYPE qsType = this.qs.getQsType();
		String engineId = this.dataframe.getEngineId();
		
		if(qsType == QUERY_STRUCT_TYPE.ENGINE && engineId.equals(this.qs.getEngineId())) {
			boolean ignore = MetadataUtility.ignoreConceptData(engineId);
			if(ignore) {
				// this join may not be defined within the QS itself
				// as we join on all properties
				appendNecessaryRels(joins);
			}
			ImportUtility.parseNativeQueryStructIntoMeta(this.dataframe, this.qs, ignore, null);
			this.dataframe.mergeQueryStruct(this.qs);
			return this.dataframe;
		} else {
			// this is the case where we are going across databases or doing some algorithm
			// need to persist the data so we can do this
			// we will flush and return a new frame to accomplish this
			return generateNewFrame(joins);
		}
	}
	
	/**
	 * Need to add additional joins that are part of the merge
	 * Since we do not really have TABLE, join, TABLE anymore in RDBMS
	 * Yet I need to know what the join is
	 * @param joins
	 */
	public void appendNecessaryRels(List<Join> joins) {
		Set<IRelation> relations = this.qs.getRelations();
		List<IQuerySelector> selectors = this.qs.getSelectors();
		
		int numJoins = joins.size();
		List<IRelation> relsToAdd = new ArrayList<>();
		
		J_LOOP : for(int i = 0; i < numJoins; i++) {
			Join j = joins.get(i);
			BasicRelationship jRel = new BasicRelationship(new String[] {j.getRColumn(), j.getJoinType(), j.getLColumn()});
			for(IRelation rel : relations) {
				if(jRel.equals(rel)) {
					continue J_LOOP;
				}
			}
			relsToAdd.add(jRel);
			// go through the selectors
			// if the qualifier is a selector
			// we want to remove it
			// since it is the join column
			Iterator<IQuerySelector> it = selectors.iterator();
			while(it.hasNext()) {
				if(it.next().getQueryStructName().equals(j.getRColumn())) {
					it.remove();
					break;
				}
			}
		}
		
		for(IRelation rel : relsToAdd) {
			this.qs.addRelation(rel);
		}
	}
	
	/**
	 * Generate a new frame from the existing native query
	 * @param joins
	 * @return
	 * @throws Exception 
	 */
	private ITableDataFrame generateNewFrame(List<Join> joins) throws Exception {
		// first, load the entire native frame into rframe
		SelectQueryStruct nativeQs = this.dataframe.getQueryStruct();
		// need to convert the native QS to properly form the RDataTable
		nativeQs = QSAliasToPhysicalConverter.getPhysicalQs(nativeQs, this.dataframe.getMetaData());
		IRawSelectWrapper nativeFrameIt = this.dataframe.query(nativeQs);
		try {
			if(!FrameSizeRetrictions.sizeWithinLimit(nativeFrameIt.getNumRecords())) {
				SemossPixelException exception = new SemossPixelException(
						new NounMetadata("Frame size is too large, please limit the data size before proceeding", 
								PixelDataType.CONST_STRING, 
								PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED, PixelOperationType.ERROR));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage());
		}
		
		RDataTable rFrame = new RDataTable(this.in.getRJavaTranslator(LogManager.getLogger(CLASS_NAME)), Utility.getRandomString(8));
		RImporter rImporter = new RImporter(rFrame, nativeQs, nativeFrameIt);
		rImporter.insertData();
		
		// now, we want to merge this new data into it
		IRawSelectWrapper mergeFrameIt = null;
		try {
			mergeFrameIt = ImportUtility.generateIterator(this.qs, this.dataframe);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					new NounMetadata("Error occurred executing query before loading into frame", 
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		try {
			if(!FrameSizeRetrictions.importWithinLimit(rFrame, mergeFrameIt)) {
				SemossPixelException exception = new SemossPixelException(
						new NounMetadata("Frame size is too large, please limit the data size before proceeding", 
								PixelDataType.CONST_STRING, 
								PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED, PixelOperationType.ERROR));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage());
		}
		
		rImporter = new RImporter(rFrame, this.qs, mergeFrameIt);
		rImporter.mergeData(joins);
		return rFrame;
	}
	
	/**
	 * This method is here to clean up and properly add the columns when there is a * in the query
	 * @param engineId
	 * @param selectors
	 * @param rels
	 */
	private void cleanUpSelectors(String engineId, List<IQuerySelector> selectors, Set<String[]> rels) {
		String origTable = null;
		boolean queryAll = false;
		int foundIndex = -1;
		
		for(int i = 0; i < selectors.size(); i++) {
			IQuerySelector s = selectors.get(i);
			if(s instanceof QueryOpaqueSelector) {
				if(((QueryOpaqueSelector)s).getQuerySelectorSyntax().equals("*")) {
					foundIndex = i;
					origTable = ((QueryOpaqueSelector)s).getTable();
					queryAll = true;
					break;
				}
			}
		}
		
		if(queryAll) {
			// query all pulls from every table
			// including the joisn
			List<String> tables = new Vector<String>();
			tables.add(origTable.toLowerCase());
			
			for(String[] r : rels) {
				String from = r[0];
				if(from.contains("__")) {
					from = from.split("__")[0];
				}
				if(!tables.contains(from)) {
					tables.add(from.toLowerCase());
				}
				
				String to = r[2];
				if(to.contains("__")) {
					to = to.split("__")[0];
				}
				if(!tables.contains(to)) {
					tables.add(to.toLowerCase());
				}
			}

			boolean multiTable = tables.size() > 1;
			Collection<String> possibleSelectors = MasterDatabaseUtility.getSelectorsWithinDatabaseRDBMS(engineId);
			for(String pSelector : possibleSelectors) {
				if(pSelector.contains("__")) {
					String possibleT = pSelector.split("__")[0];
					if(tables.contains(possibleT.toLowerCase())) {
						if(multiTable) {
							qs.addSelector(new QueryColumnSelector(pSelector, pSelector));
						} else {
							qs.addSelector(new QueryColumnSelector(pSelector));
						}
					}
				}
			}
			
			// now we need to remove the index that we had found
			qs.getSelectors().remove(foundIndex);
		}
	}
}
