package prerna.reactor.insights.copy;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.r.RDataTable;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CopyFrameUtil {

	private static final String CLASS_NAME = CopyFrameUtil.class.getName();

	private CopyFrameUtil() {

	}

	/**
	 * Copy the frame into a new frame
	 * Will also account for R/Py to rename the variable since the insight copy will
	 * share the same space
	 * @param insightContext
	 * @param frameToCopy
	 * @param limit
	 * @return
	 * @throws Exception 
	 */
	public static ITableDataFrame copyFrame(Insight insightContext, ITableDataFrame frameToCopy, int limit) throws Exception {
		String oldName = frameToCopy.getName();
		String newName = oldName + "_COPY";

		// one thing that is consistent across all frames
		OwlTemporalEngineMeta newMetadata = frameToCopy.getMetaData().copy();

		ITableDataFrame newFrame =  null;
		try {
			// we need to set the correct context for the pandas + data.table frames
			// we will also account for names to be new

			if(frameToCopy instanceof PandasFrame) {
				newFrame = new PandasFrame(newName);
				// set the metadata
				newFrame.setMetaData(newMetadata);
				PandasFrame dt = (PandasFrame) newFrame;

				dt.setJep( insightContext.getPy() );
				dt.setTranslator( insightContext.getPyTranslator() );
				if(limit > 0) {
					dt.runScript(newName + " = " + oldName + "[:" + limit + "].copy(deep=True)");
				} else {
					dt.runScript(newName + " = " + oldName + ".copy(deep=True)");
				}
				// also do the wrapper
				dt.setName(newName);
				// the wrapper name is auto generated when you set name
				String newWrapperName = dt.getWrapperName();
				dt.runScript(PandasSyntaxHelper.makeWrapper(newWrapperName, newName));
				dt.getMetaData().modifyVertexName(oldName, newName);

			} else if (frameToCopy instanceof RDataTable) {
				newFrame = new RDataTable( insightContext.getRJavaTranslator(CLASS_NAME), newName );
				// set the metadata
				newFrame.setMetaData(newMetadata);
				RDataTable dt = (RDataTable) newFrame;

				if(limit > 0) {
					dt.executeRScript(newName + "<- " + oldName + "[1:" + limit + ", ]");
				} else {
					dt.executeRScript(newName + "<- " + oldName);
				}
				dt.setName(newName);
				dt.getMetaData().modifyVertexName(oldName, newName);

			} else if(frameToCopy instanceof NativeFrame) {
				newFrame = new NativeFrame();
				// set the name
				// add the query struct
				// no need to limit on native frame?
				newFrame.setName(oldName);
				((NativeFrame) newFrame).mergeQueryStruct( ((NativeFrame) frameToCopy).getQueryStruct() );
			}

			else {
				newFrame = (ITableDataFrame) Class.forName(frameToCopy.getClass().getName()).newInstance();
				newFrame.setName(oldName);
				// just do a query on the current frame
				SelectQueryStruct qs = newMetadata.getFlatTableQs(false);
				// add the limit
				qs.setLimit(limit);
				IRawSelectWrapper iterator = frameToCopy.query(qs);
				// just the existing insert logic
				IImporter importer = ImportFactory.getImporter(newFrame, qs, iterator);
				importer.insertData(newMetadata);
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		return newFrame;
	}

	public static ITableDataFrame renameFrame(ITableDataFrame frame, String newName) {
		String oldName = frame.getName();

		// one thing that is consistent across all frames
		OwlTemporalEngineMeta metadata = frame.getMetaData();

		// we need to set the correct context for the pandas + data.table frames
		// we will also account for names to be new
		if(frame instanceof PandasFrame) {
			((PandasFrame) frame).runScript(newName + " = " + oldName + ".copy(deep=True)");
			((PandasFrame) frame).runScript("del " + oldName);
			// set the name first
			frame.setName(newName);
			((PandasFrame) frame).runScript(PandasSyntaxHelper.makeWrapper(((PandasFrame) frame).getWrapperName(), newName));

			metadata.modifyVertexName(oldName, newName);

		} else if (frame instanceof RDataTable) {
			((RDataTable) frame).executeRScript(newName + "<- " + oldName);
			((RDataTable) frame).executeRScript("rm(" + oldName + ")");
			frame.setName(newName);
			
			metadata.modifyVertexName(oldName, newName);

		} else if (frame instanceof AbstractRdbmsFrame) {
			
			// TODO: NEED TO COPY THE TABLE
			
			frame.setName(newName);
			metadata.modifyVertexName(oldName, newName);
			
		} else {
			// set the name
			// that is it
			// this is native and graph
			frame.setName(newName);
		}

		return frame;
	}

	/**
	 * Merge the frame filters into the new frame
	 * @param frame
	 * @param newFrame
	 */
	public static void copyFrameFilters(ITableDataFrame frame, ITableDataFrame newFrame) {
		List<IQueryFilter> grf = frame.getFrameFilters().getFilters();
		for(IQueryFilter ifilter : grf) {
			if(ifilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
				// basically need to replace any physical column names to the alias
				SimpleQueryFilter simpleF = (SimpleQueryFilter) ifilter;
				if(simpleF.getSimpleFilterType() == SimpleQueryFilter.FILTER_TYPE.COL_TO_COL) {
					IQuerySelector lhs = (IQuerySelector) simpleF.getLComparison().getValue();
					IQuerySelector rhs = (IQuerySelector) simpleF.getRComparison().getValue();

					NounMetadata newLhs = null;
					NounMetadata newRhs = null;
					
					if(lhs instanceof QueryColumnSelector) {
						String col = ((QueryColumnSelector) lhs).getColumn();
						if(! (col == null || col.isEmpty() || col.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) ) {
							// i need to replace you
							String alias = frame.getMetaData().getAliasFromUniqueName(lhs.getQueryStructName());
							if(alias != null) {
								newLhs = new NounMetadata(new QueryColumnSelector(alias), PixelDataType.COLUMN);
							}
						}
					}
					
					if(rhs instanceof QueryColumnSelector) {
						String col = ((QueryColumnSelector) rhs).getColumn();
						if(! (col == null || col.isEmpty() || col.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) ) {
							// i need to replace you
							String alias = frame.getMetaData().getAliasFromUniqueName(rhs.getQueryStructName());
							if(alias != null) {
								newRhs = new NounMetadata(new QueryColumnSelector(alias), PixelDataType.COLUMN);
							}
						}
					}
					
					if(newLhs != null && newRhs != null) {
						SimpleQueryFilter newFilter = new SimpleQueryFilter(newLhs, simpleF.getComparator(), newRhs);
						newFrame.addFilter(newFilter);
					} else if(newLhs != null) {
						SimpleQueryFilter newFilter = new SimpleQueryFilter(newLhs, simpleF.getComparator(), simpleF.getRComparison());
						newFrame.addFilter(newFilter);
					} else if(newRhs != null) {
						SimpleQueryFilter newFilter = new SimpleQueryFilter(simpleF.getLComparison(), simpleF.getComparator(), newRhs);
						newFrame.addFilter(newFilter);
					} else {
						newFrame.addFilter(ifilter);
					}
					
				} else if(simpleF.getSimpleFilterType() == SimpleQueryFilter.FILTER_TYPE.COL_TO_VALUES) {
					IQuerySelector lhs = (IQuerySelector) simpleF.getLComparison().getValue();
					NounMetadata newLhs = null;
					if(lhs instanceof QueryColumnSelector) {
						String col = ((QueryColumnSelector) lhs).getColumn();
						if(! (col == null || col.isEmpty() || col.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) ) {
							// i need to replace you
							String alias = frame.getMetaData().getAliasFromUniqueName(lhs.getQueryStructName());
							if(alias != null) {
								newLhs = new NounMetadata(new QueryColumnSelector(alias), PixelDataType.COLUMN);
							}
						}
					}
					
					if(newLhs != null) {
						SimpleQueryFilter newFilter = new SimpleQueryFilter(newLhs, simpleF.getComparator(), simpleF.getRComparison());
						newFrame.addFilter(newFilter);
					} else {
						newFrame.addFilter(ifilter);
					}
					
					
				} else if(simpleF.getSimpleFilterType() == SimpleQueryFilter.FILTER_TYPE.COL_TO_VALUES) {
					IQuerySelector rhs = (IQuerySelector) simpleF.getRComparison().getValue();
					NounMetadata newRhs = null;
					if(rhs instanceof QueryColumnSelector) {
						String col = ((QueryColumnSelector) rhs).getColumn();
						if(! (col == null || col.isEmpty() || col.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) ) {
							// i need to replace you
							String alias = frame.getMetaData().getAliasFromUniqueName(rhs.getQueryStructName());
							if(alias != null) {
								newRhs = new NounMetadata(new QueryColumnSelector(alias), PixelDataType.COLUMN);
							}
						}
					}
					
					if(newRhs != null) {
						SimpleQueryFilter newFilter = new SimpleQueryFilter(simpleF.getLComparison(), simpleF.getComparator(), newRhs);
						newFrame.addFilter(newFilter);
					} else {
						newFrame.addFilter(ifilter);
					}
					
				} else {
					// welp, i dont feel like recursively going through this
					// if you are doing this, i hope you made sure the cache is accurate
					newFrame.addFilter(ifilter);
				}
			} else {
				// welp, i dont feel like recursively going through this
				// if you are doing this, i hope you made sure the cache is accurate
				newFrame.addFilter(ifilter);
			}
		}
	}
	
}
