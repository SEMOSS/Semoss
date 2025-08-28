/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.ds.r;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.util.Utility;

public class RScaledUniqueFrameIterator implements Iterator<List<Object[]>> {

	private RFrameBuilder builder;
	private OwlTemporalEngineMeta metaData;

	private String uniqueColumnName;
	private String frameColumnName;
	private Iterator<Object> valueIterator;

	private String tempVarName;
	private String[] headers = null;

	public RScaledUniqueFrameIterator(RDataTable frame, RFrameBuilder builder, String columnName, Double[] maxArr,
			Double[] minArr, List<SemossDataType> dataTypes, List<String> selectors) {

		this.builder = builder;
		this.metaData = frame.getMetaData();

		this.uniqueColumnName = this.metaData.getUniqueNameFromAlias(columnName);
		if (this.uniqueColumnName == null) {
			this.uniqueColumnName = columnName;
		}
		String[] split = this.uniqueColumnName.split("__");
		this.frameColumnName = split[1];

		Object[] column = frame.getColumn(this.uniqueColumnName);
		this.valueIterator = Arrays.asList(column).iterator();

		// create the QS being used for querying
		SelectQueryStruct qs = new SelectQueryStruct();
		int numSelectors = selectors.size();
		for (int i = 0; i < numSelectors; i++) {
			String unqiueSelectorName = selectors.get(i);

			QueryColumnSelector sColumn = new QueryColumnSelector();
			if (unqiueSelectorName.contains("__")) {
				String[] sSplit = unqiueSelectorName.split("__");
				sColumn.setTable(sSplit[0]);
				sColumn.setColumn(sSplit[1]);
			} else {
				sColumn.setTable(unqiueSelectorName);
				sColumn.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
			}

			if (maxArr[i] != null && minArr[i] != null) {
				// we need to normalize this guy
				double range = maxArr[i] - minArr[i];

				QueryConstantSelector minConst = new QueryConstantSelector();
				minConst.setConstant(minArr[i]);

				QueryArithmeticSelector minusSelector = new QueryArithmeticSelector();
				minusSelector.setLeftSelector(sColumn);
				minusSelector.setRightSelector(minConst);
				minusSelector.setMathExpr("-");

				QueryConstantSelector rangeConst = new QueryConstantSelector();
				rangeConst.setConstant(range);

				QueryArithmeticSelector normalizeSelector = new QueryArithmeticSelector();
				normalizeSelector.setLeftSelector(minusSelector);
				normalizeSelector.setRightSelector(rangeConst);
				normalizeSelector.setMathExpr("/");

				qs.addSelector(normalizeSelector);
			} else {
				// we just add this as a column
				qs.addSelector(sColumn);
			}
		}

		// dont forget about filters
		qs.mergeImplicitFilters(frame.getFrameFilters());

		qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, this.metaData);
		RInterpreter interp = new RInterpreter();
		interp.setQueryStruct(qs);
		interp.setDataTableName(frame.getName());
		interp.setColDataTypes(this.metaData.getHeaderToTypeMap());
		String rQuery = interp.composeQuery();

		this.tempVarName = "temp" + Utility.getRandomString(6);
		String tempVarQuery = this.tempVarName + " <- {" + rQuery + "}";
		this.builder.evalR(tempVarQuery);
		this.headers = builder.getColumnNames(tempVarName);
	}

	@Override
	public boolean hasNext() {
		if (this.valueIterator.hasNext()) {
			return true;
		}
		return false;
	}

	@Override
	public List<Object[]> next() {
		if (hasNext()) {
			Object nextInstance = this.valueIterator.next();
			String quote = "";
			if (!(nextInstance instanceof Number)) {
				quote = "\"";
			}
			String query = this.tempVarName + "[" + this.frameColumnName + " == " + quote + nextInstance + quote
					+ ", ]";
			List<Object[]> data = this.builder.getBulkDataRow(query, headers);
			return data;
		} else {
			throw new NoSuchElementException("No more elements");
		}
	}
}
