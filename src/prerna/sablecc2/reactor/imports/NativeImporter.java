package prerna.sablecc2.reactor.imports;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;

import net.snowflake.client.jdbc.internal.org.bouncycastle.util.Arrays;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.impl.util.MetadataUtility;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.parsers.OpaqueSqlParser;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class NativeImporter extends AbstractImporter {

	private static final String CLASS_NAME = NativeImporter.class.getName();
	
	private NativeFrame dataframe;
	private SelectQueryStruct qs;
	
	public NativeImporter(NativeFrame dataframe, SelectQueryStruct qs) {
		this.dataframe = dataframe;
		this.qs = qs;
	}
	
	@Override
	public void insertData() {
		// see if we can parse the query into a valid qs object
		if(this.qs.getQsType() == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY && 
				this.qs.retrieveQueryStructEngine() instanceof RDBMSNativeEngine) {
			// lets see what happens
			OpaqueSqlParser parser = new OpaqueSqlParser();
//			SqlParser parser = new SqlParser();
			String query = ((HardSelectQueryStruct) this.qs).getQuery();
			try {
				SelectQueryStruct newQs = this.qs.getNewBaseQueryStruct();
				newQs.merge(parser.processQuery(query));
				// we were able to parse successfully
				// override the reference
				this.qs = newQs;
				this.qs.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
				
				// we need to figure out the columns if there is a *
				cleanUpSelectors(this.qs.getEngineId(), this.qs.getSelectors(), this.qs.getRelations());
			} catch (Exception e) {
				// we were not successful in parsing :/
				e.printStackTrace();
			}
		}
		boolean ignore = MetadataUtility.ignoreConceptData(this.qs.getEngineId());
		ImportUtility.parseNativeQueryStructIntoMeta(this.dataframe, this.qs, ignore);
		this.dataframe.mergeQueryStruct(this.qs);
	}

	@Override
	public void insertData(OwlTemporalEngineMeta metaData) {
		this.dataframe.setMetaData(metaData);
		this.dataframe.mergeQueryStruct(this.qs);
	}

	@Override
	public ITableDataFrame mergeData(List<Join> joins) {
		QUERY_STRUCT_TYPE qsType = this.qs.getQsType();
		String engineId = this.dataframe.getEngineId();
		if(qsType == QUERY_STRUCT_TYPE.ENGINE && engineId.equals(this.qs.getEngineId())) {
			boolean ignore = MetadataUtility.ignoreConceptData(engineId);
			if(ignore) {
				// this join may not be defined within the QS itself
				// as we join on all properties
				appendNecessaryRels(joins);
			}
			ImportUtility.parseNativeQueryStructIntoMeta(this.dataframe, this.qs, ignore);
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
	private void appendNecessaryRels(List<Join> joins) {
		Set<String[]> relations = this.qs.getRelations();
		List<IQuerySelector> selectors = this.qs.getSelectors();
		
		int numJoins = joins.size();
		List<String[]> relsToAdd = new Vector<String[]>();
		
		J_LOOP : for(int i = 0; i < numJoins; i++) {
			Join j = joins.get(i);
			String[] jRel = new String[] {j.getQualifier(), j.getJoinType(), j.getSelector()};
			for(String[] rel : relations) {
				if(Arrays.areEqual(rel, jRel)) {
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
				if(it.next().getQueryStructName().equals(j.getQualifier())) {
					it.remove();
					break;
				}
			}
		}
		
		for(String[] rel : relsToAdd) {
			this.qs.addRelation(rel[0], rel[2], rel[1]);
		}
	}
	
	/**
	 * Generate a new frame from the existing native query
	 * @param joins
	 * @return
	 */
	private ITableDataFrame generateNewFrame(List<Join> joins) {
		// first, load the entire native frame into rframe
		SelectQueryStruct nativeQs = this.dataframe.getQueryStruct();
		// need to convert the native QS to properly form the RDataTable
		nativeQs = QSAliasToPhysicalConverter.getPhysicalQs(nativeQs, this.dataframe.getMetaData());
		IRawSelectWrapper nativeFrameIt = this.dataframe.query(nativeQs);
		if(!ImportSizeRetrictions.sizeWithinLimit(nativeFrameIt.getNumRecords())) {
			SemossPixelException exception = new SemossPixelException(
					new NounMetadata("Frame size is too large, please limit the data size before proceeding", 
							PixelDataType.CONST_STRING, 
							PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		
		RDataTable rFrame = new RDataTable(this.in.getRJavaTranslator(LogManager.getLogger(CLASS_NAME)), Utility.getRandomString(8));
		RImporter rImporter = new RImporter(rFrame, nativeQs, nativeFrameIt);
		rImporter.insertData();
		
		// now, we want to merge this new data into it
		IRawSelectWrapper mergeFrameIt = null;
		try {
			mergeFrameIt = ImportUtility.generateIterator(this.qs, this.dataframe);
		} catch (Exception e) {
			e.printStackTrace();
			throw new SemossPixelException(
					new NounMetadata("Error occured executing query before loading into frame", 
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		if(!ImportSizeRetrictions.mergeWithinLimit(rFrame, mergeFrameIt)) {
			SemossPixelException exception = new SemossPixelException(
					new NounMetadata("Frame size is too large, please limit the data size before proceeding", 
							PixelDataType.CONST_STRING, 
							PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
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
			Collection<String> possibleSelectors = MasterDatabaseUtility.getSelectorsWithinEngineRDBMS(engineId);
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
