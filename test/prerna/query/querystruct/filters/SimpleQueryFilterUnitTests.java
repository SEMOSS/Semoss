/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.query.querystruct.filters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.google.re2j.PatternSyntaxException;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter.QUERY_FILTER_TYPE;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Covers the single comparison filter: how it classifies the two sides of the
 * comparison, how two filters on the same column combine or cancel each other
 * out, how regex comparators decide that two filters overlap, how it renders
 * itself for the front end, and the static helpers that build one.
 */
public class SimpleQueryFilterUnitTests {

	private static final String COL = "TABLE__COLUMN";
	private static final String OTHER_COL = "OTHER__FIELD";

	@Nested
	class Construction {

		@Test
		void normalizesDiamondComparatorToNotEquals() {
			assertEquals("!=", colToVal("<>", "a").getComparator());
		}

		@Test
		void keepsEveryOtherComparatorAsGiven() {
			assertEquals("==", colToVal("==", "a").getComparator());
			assertEquals("!=", colToVal("!=", "a").getComparator());
			assertEquals("?like", colToVal("?like", "a").getComparator());
			assertEquals(">=", colToVal(COL, ">=", 5, PixelDataType.CONST_INT).getComparator());
		}

		@Test
		void exposesBothSidesOfTheComparison() {
			NounMetadata left = new NounMetadata(new QueryColumnSelector(COL), PixelDataType.COLUMN);
			NounMetadata right = new NounMetadata("a", PixelDataType.CONST_STRING);
			SimpleQueryFilter filter = new SimpleQueryFilter(left, "==", right);

			assertSame(left, filter.getLComparison());
			assertSame(right, filter.getRComparison());
		}

		@Test
		void settersReplaceTheStoredComparisons() {
			SimpleQueryFilter filter = colToVal("==", "a");

			NounMetadata newLeft = new NounMetadata(new QueryColumnSelector(OTHER_COL), PixelDataType.COLUMN);
			NounMetadata newRight = new NounMetadata("b", PixelDataType.CONST_STRING);
			filter.setLComparison(newLeft);
			filter.setRComparison(newRight);
			filter.setComparator("?like");

			assertSame(newLeft, filter.getLComparison());
			assertSame(newRight, filter.getRComparison());
			assertEquals("?like", filter.getComparator());
		}

		@Test
		void alwaysReportsItselfAsASimpleFilter() {
			assertEquals(QUERY_FILTER_TYPE.SIMPLE, colToVal("==", "a").getQueryFilterType());
		}
	}

	@Nested
	class FilterTypeClassification {

		@Test
		void classifiesColumnToColumn() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			assertEquals(FILTER_TYPE.COL_TO_COL, filter.getSimpleFilterType());
		}

		@Test
		void classifiesColumnToValues() {
			assertEquals(FILTER_TYPE.COL_TO_VALUES, colToVal("==", "a").getSimpleFilterType());
		}

		@Test
		void treatsEveryConstantAndDataSetTypeAsAValue() {
			PixelDataType[] valueTypes = { PixelDataType.CONST_STRING, PixelDataType.CONST_INT,
					PixelDataType.CONST_DECIMAL, PixelDataType.CONST_DATE, PixelDataType.CONST_TIMESTAMP,
					PixelDataType.BOOLEAN, PixelDataType.NULL_VALUE, PixelDataType.FORMATTED_DATA_SET,
					PixelDataType.TASK };

			for (PixelDataType type : valueTypes) {
				assertEquals(FILTER_TYPE.COL_TO_VALUES, colToVal(COL, "==", "a", type).getSimpleFilterType(),
						"expected " + type + " to count as a value");
			}
		}

		@Test
		void classifiesValuesToColumn() {
			assertEquals(FILTER_TYPE.VALUES_TO_COL,
					valToCol("a", "==", COL, PixelDataType.CONST_STRING).getSimpleFilterType());
		}

		@Test
		void classifiesColumnToSubQueryInBothDirections() {
			SelectQueryStruct subQuery = new SelectQueryStruct();
			SimpleQueryFilter colToQuery = SimpleQueryFilter.makeColToSubQuery(COL, "==", subQuery);
			SimpleQueryFilter queryToCol = new SimpleQueryFilter(new NounMetadata(subQuery, PixelDataType.QUERY_STRUCT),
					"==", new NounMetadata(new QueryColumnSelector(COL), PixelDataType.COLUMN));

			assertEquals(FILTER_TYPE.COL_TO_QUERY, colToQuery.getSimpleFilterType());
			assertEquals(FILTER_TYPE.QUERY_TO_COL, queryToCol.getSimpleFilterType());
		}

		@Test
		void classifiesValueToValue() {
			SimpleQueryFilter filter = new SimpleQueryFilter(new NounMetadata(1, PixelDataType.CONST_INT), "==",
					new NounMetadata(2, PixelDataType.CONST_INT));

			assertEquals(FILTER_TYPE.VALUE_TO_VALUE, filter.getSimpleFilterType());
		}

		@Test
		void classifiesLambdaComparisonsInBothDirections() {
			NounMetadata column = new NounMetadata(new QueryColumnSelector(COL), PixelDataType.COLUMN);
			NounMetadata lambda = new NounMetadata("someLambda", PixelDataType.LAMBDA);

			assertEquals(FILTER_TYPE.COL_TO_LAMBDA, new SimpleQueryFilter(column, "==", lambda).getSimpleFilterType());
			assertEquals(FILTER_TYPE.LAMBDA_TO_COL, new SimpleQueryFilter(lambda, "==", column).getSimpleFilterType());
		}

		@Test
		void returnsNoTypeForUnrecognizedCombinations() {
			SimpleQueryFilter filter = new SimpleQueryFilter(
					new NounMetadata(Collections.emptyMap(), PixelDataType.MAP), "==",
					new NounMetadata(Collections.emptyMap(), PixelDataType.MAP));

			assertNull(filter.getSimpleFilterType());
		}
	}

	@Nested
	class Merging {

		@Test
		void unionsValuesForAdditiveComparators() {
			SimpleQueryFilter filter = colToVal(COL, "==", values("a", "b"), PixelDataType.CONST_STRING);
			filter.merge(colToVal(COL, "==", values("b", "c"), PixelDataType.CONST_STRING));

			assertEquals(Arrays.asList("a", "b", "c"), filter.getRComparison().getValue());
			assertEquals(PixelDataType.CONST_STRING, filter.getRComparison().getNounType());
		}

		@Test
		void unionsScalarValuesWithoutDuplicating() {
			SimpleQueryFilter filter = colToVal("?like", "warn");
			filter.merge(colToVal("?like", "warn"));

			assertEquals(Arrays.asList("warn"), filter.getRComparison().getValue());
		}

		@Test
		void unionsAcrossMirroredFilterSides() {
			SimpleQueryFilter filter = colToVal(COL, "==", values("a"), PixelDataType.CONST_STRING);
			filter.merge(valToCol(values("b"), "==", COL, PixelDataType.CONST_STRING));

			assertEquals(Arrays.asList("a", "b"), filter.getRComparison().getValue());
		}

		@Test
		void unionsValuesWhenBothFiltersLeadWithValues() {
			SimpleQueryFilter filter = valToCol(values("a", "b"), "==", COL, PixelDataType.CONST_STRING);
			filter.merge(valToCol(values("c"), "==", COL, PixelDataType.CONST_STRING));

			assertEquals(Arrays.asList("a", "b", "c"), filter.getLComparison().getValue());
		}

		@Test
		void unionsAcrossMirroredFilterSidesWhenValuesComeFirst() {
			SimpleQueryFilter filter = valToCol(values("a"), "==", COL, PixelDataType.CONST_STRING);
			filter.merge(colToVal(COL, "==", values("b"), PixelDataType.CONST_STRING));

			assertEquals(Arrays.asList("a", "b"), filter.getLComparison().getValue());
		}

		@Test
		void keepsTheLargerBoundForGreaterThanComparators() {
			SimpleQueryFilter filter = colToVal(COL, ">", 5, PixelDataType.CONST_INT);
			filter.merge(colToVal(COL, ">", 10, PixelDataType.CONST_INT));

			assertEquals(10, filter.getRComparison().getValue());

			SimpleQueryFilter inclusive = colToVal(COL, ">=", 10, PixelDataType.CONST_INT);
			inclusive.merge(colToVal(COL, ">=", 5, PixelDataType.CONST_INT));

			assertEquals(10, inclusive.getRComparison().getValue());
		}

		@Test
		void keepsTheSmallerBoundForLessThanComparators() {
			SimpleQueryFilter filter = colToVal(COL, "<", 10, PixelDataType.CONST_INT);
			filter.merge(colToVal(COL, "<", 5, PixelDataType.CONST_INT));

			assertEquals(5, filter.getRComparison().getValue());

			SimpleQueryFilter inclusive = colToVal(COL, "<=", 5, PixelDataType.CONST_INT);
			inclusive.merge(colToVal(COL, "<=", 10, PixelDataType.CONST_INT));

			assertEquals(5, inclusive.getRComparison().getValue());
		}

		@Test
		void readsTheBoundOutOfAListForNumericComparators() {
			SimpleQueryFilter filter = colToVal(COL, ">", values(5), PixelDataType.CONST_INT);
			filter.merge(colToVal(COL, ">", values(10), PixelDataType.CONST_INT));

			assertEquals(10, filter.getRComparison().getValue());
		}

		@Test
		void refusesToMergeFiltersWithDifferentComparators() {
			SimpleQueryFilter filter = colToVal("==", "a");

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> filter.merge(colToVal("?like", "a")));
			assertEquals("Cannot merge these filters. Comparators must match.", e.getMessage());
		}

		@Test
		void treatsDiamondAndNotEqualsAsTheSameComparator() {
			SimpleQueryFilter filter = colToVal(COL, "<>", values("a"), PixelDataType.CONST_STRING);
			filter.merge(colToVal(COL, "!=", values("b"), PixelDataType.CONST_STRING));

			assertEquals(Arrays.asList("a", "b"), filter.getRComparison().getValue());
		}

		@Test
		void refusesToMergeFiltersThatAreNotAboutColumnsAndValues() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> filter.merge(SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL)));
			assertEquals("Unable to merge these filters", e.getMessage());
		}
	}

	@Nested
	class ComparatorReversal {

		@Test
		void flipsTheComparatorInPlace() {
			SimpleQueryFilter equality = colToVal("==", "a");
			equality.reverseComparator();
			assertEquals("!=", equality.getComparator());

			SimpleQueryFilter greaterThan = colToVal(COL, ">", 5, PixelDataType.CONST_INT);
			greaterThan.reverseComparator();
			assertEquals("<", greaterThan.getComparator());

			SimpleQueryFilter like = colToVal("?like", "a");
			like.reverseComparator();
			assertEquals("?nlike", like.getComparator());
		}
	}

	@Nested
	class EquivalentColumnModification {

		@Test
		void matchesWhenBothFiltersFilterTheSameColumnTheSameWay() {
			assertTrue(colToVal("==", "a").equivalentColumnModifcation(colToVal("==", "b")));
		}

		@Test
		void doesNotMatchWhenComparatorsDifferUnlessComparatorsAreIgnored() {
			SimpleQueryFilter filter = colToVal("==", "a");
			SimpleQueryFilter other = colToVal("?like", "a");

			assertFalse(filter.equivalentColumnModifcation(other));
			assertTrue(filter.equivalentColumnModifcation(other, false));
		}

		@Test
		void doesNotMatchWhenTheColumnsDiffer() {
			SimpleQueryFilter filter = colToVal(COL, "==", "a", PixelDataType.CONST_STRING);
			SimpleQueryFilter other = colToVal(OTHER_COL, "==", "a", PixelDataType.CONST_STRING);

			assertFalse(filter.equivalentColumnModifcation(other));
		}

		@Test
		void matchesRegardlessOfWhichSideHoldsTheColumn() {
			SimpleQueryFilter colFirst = colToVal("==", "a");
			SimpleQueryFilter valuesFirst = valToCol("b", "==", COL, PixelDataType.CONST_STRING);

			assertTrue(colFirst.equivalentColumnModifcation(valuesFirst));
			assertTrue(valuesFirst.equivalentColumnModifcation(colFirst));
			assertTrue(valuesFirst.equivalentColumnModifcation(valToCol("c", "==", COL, PixelDataType.CONST_STRING)));
		}

		@Test
		void doesNotMatchFiltersThatAreNotAboutColumnsAndValues() {
			SimpleQueryFilter colToCol = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			assertFalse(colToCol.equivalentColumnModifcation(colToVal("==", "a")));
			assertFalse(colToVal("==", "a").equivalentColumnModifcation(colToCol));
		}
	}

	@Nested
	class OverlappingRegexValues {

		@Test
		void matchesWhenOtherFilterContainsRegex() {
			SimpleQueryFilter literalFilter = colToVal("==", "release-warning.log");
			SimpleQueryFilter regexFilter = colToVal("?like", "warn(?:ing)?");

			assertTrue(literalFilter.isOverlappingRegexValues(regexFilter, true));
		}

		@Test
		void matchesWhenThisFilterContainsRegexAndComparisonIsBidirectional() {
			SimpleQueryFilter regexFilter = colToVal("?like", "warn(?:ing)?");
			SimpleQueryFilter literalFilter = colToVal("==", "release-warning.log");

			assertTrue(regexFilter.isOverlappingRegexValues(literalFilter, true));
			assertFalse(regexFilter.isOverlappingRegexValues(literalFilter, false));
		}

		@Test
		void preservesCaseInsensitiveMatching() {
			SimpleQueryFilter literalFilter = colToVal("==", "BUILD FAILURE");
			SimpleQueryFilter regexFilter = colToVal("?like", "failure");

			assertTrue(literalFilter.isOverlappingRegexValues(regexFilter, true));
		}

		@Test
		void rejectsUnsupportedLookaroundExpressions() {
			SimpleQueryFilter literalFilter = colToVal("==", "secretvalue");
			SimpleQueryFilter regexFilter = colToVal("?like", "secret(?=value)");

			assertThrows(PatternSyntaxException.class, () -> literalFilter.isOverlappingRegexValues(regexFilter, true));
		}

		@Test
		void evaluatesNestedQuantifiersInLinearTime() {
			SimpleQueryFilter literalFilter = colToVal("==", "a".repeat(200_000) + "!");
			SimpleQueryFilter regexFilter = colToVal("?like", "(a+)+$");

			assertTimeoutPreemptively(Duration.ofSeconds(1),
					() -> assertFalse(literalFilter.isOverlappingRegexValues(regexFilter, true)));
		}

		@Test
		void doesNotOverlapWhenNeitherComparatorIsRegex() {
			assertFalse(colToVal("==", "a").isOverlappingRegexValues(colToVal("==", "a"), true));
		}

		@Test
		void doesNotOverlapWhenTheRegexDoesNotMatchAnyValue() {
			SimpleQueryFilter literalFilter = colToVal(COL, "==", values("alpha", "beta"), PixelDataType.CONST_STRING);
			SimpleQueryFilter regexFilter = colToVal("?like", "gamma");

			assertFalse(literalFilter.isOverlappingRegexValues(regexFilter, true));
		}

		@Test
		void doesNotOverlapWhenAFilterHasNoValuesToCompare() {
			SimpleQueryFilter colToCol = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			assertFalse(colToCol.isOverlappingRegexValues(colToVal("?like", "a"), true));
		}
	}

	@Nested
	class SubtractingInstanceValues {

		@Test
		void removesTheOtherFiltersValuesFromThisOne() {
			SimpleQueryFilter filter = colToVal(COL, "==", values("a", "b", "c"), PixelDataType.CONST_STRING);

			assertTrue(filter.subtractInstanceFilters(colToVal(COL, "==", values("b"), PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("a", "c"), filter.getRComparison().getValue());
		}

		@Test
		void reportsNoChangeWhenThereIsNothingToRemove() {
			SimpleQueryFilter filter = colToVal(COL, "==", values("a"), PixelDataType.CONST_STRING);

			assertFalse(filter.subtractInstanceFilters(colToVal(COL, "==", values("z"), PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("a"), filter.getRComparison().getValue());
		}

		@Test
		void promotesScalarValuesToAListBeforeRemoving() {
			SimpleQueryFilter filter = colToVal("==", "a");

			assertTrue(filter.subtractInstanceFilters(colToVal("==", "a")));
			assertEquals(Collections.emptyList(), filter.getRComparison().getValue());
			assertTrue(filter.isEmptyFilterValues());
		}

		@Test
		void removesFromTheLeftSideWhenBothFiltersLeadWithValues() {
			SimpleQueryFilter filter = valToCol(values("a", "b"), "==", COL, PixelDataType.CONST_STRING);

			assertTrue(filter.subtractInstanceFilters(valToCol(values("a"), "==", COL, PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("b"), filter.getLComparison().getValue());
		}

		@Test
		void removesAcrossMirroredFilterSides() {
			SimpleQueryFilter colFirst = colToVal(COL, "==", values("a", "b"), PixelDataType.CONST_STRING);
			assertTrue(colFirst.subtractInstanceFilters(valToCol(values("a"), "==", COL, PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("b"), colFirst.getRComparison().getValue());

			SimpleQueryFilter valuesFirst = valToCol(values("a", "b"), "==", COL, PixelDataType.CONST_STRING);
			assertTrue(
					valuesFirst.subtractInstanceFilters(colToVal(COL, "==", values("b"), PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("a"), valuesFirst.getLComparison().getValue());
		}

		@Test
		void leavesFiltersThatAreNotAboutColumnsAndValuesAlone() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			assertFalse(filter.subtractInstanceFilters(colToVal("==", "a")));
		}
	}

	@Nested
	class RetainingInstanceValues {

		@Test
		void keepsOnlyTheValuesSharedWithTheOtherFilter() {
			SimpleQueryFilter filter = colToVal(COL, "==", values("a", "b", "c"), PixelDataType.CONST_STRING);

			assertTrue(
					filter.onlyRetainInstanceValues(colToVal(COL, "==", values("b", "c"), PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("b", "c"), filter.getRComparison().getValue());
		}

		@Test
		void reportsNoChangeWhenEveryValueIsShared() {
			SimpleQueryFilter filter = colToVal(COL, "==", values("a", "b"), PixelDataType.CONST_STRING);

			assertFalse(
					filter.onlyRetainInstanceValues(colToVal(COL, "==", values("a", "b"), PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("a", "b"), filter.getRComparison().getValue());
		}

		@Test
		void retainsOnTheLeftSideWhenBothFiltersLeadWithValues() {
			SimpleQueryFilter filter = valToCol(values("a", "b"), "==", COL, PixelDataType.CONST_STRING);

			assertTrue(filter.onlyRetainInstanceValues(valToCol(values("b"), "==", COL, PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("b"), filter.getLComparison().getValue());
		}

		@Test
		void retainsAcrossMirroredFilterSides() {
			SimpleQueryFilter colFirst = colToVal(COL, "==", values("a", "b"), PixelDataType.CONST_STRING);
			assertTrue(colFirst.onlyRetainInstanceValues(valToCol(values("a"), "==", COL, PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("a"), colFirst.getRComparison().getValue());

			SimpleQueryFilter valuesFirst = valToCol(values("a", "b"), "==", COL, PixelDataType.CONST_STRING);
			assertTrue(
					valuesFirst.onlyRetainInstanceValues(colToVal(COL, "==", values("b"), PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("b"), valuesFirst.getLComparison().getValue());
		}

		@Test
		void leavesFiltersThatAreNotAboutColumnsAndValuesAlone() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			assertFalse(filter.onlyRetainInstanceValues(colToVal("==", "a")));
		}
	}

	@Nested
	class AddingInstanceValues {

		@Test
		void appendsTheOtherFiltersValues() {
			SimpleQueryFilter filter = colToVal(COL, "==", values("a"), PixelDataType.CONST_STRING);

			assertTrue(filter.addInstanceFilters(colToVal(COL, "==", values("b", "c"), PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("a", "b", "c"), filter.getRComparison().getValue());
		}

		@Test
		void reportsNoChangeWhenTheOtherFilterHasNoValues() {
			SimpleQueryFilter filter = colToVal(COL, "==", values("a"), PixelDataType.CONST_STRING);

			assertFalse(filter.addInstanceFilters(colToVal(COL, "==", values(), PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("a"), filter.getRComparison().getValue());
		}

		@Test
		void appendsOnTheLeftSideWhenBothFiltersLeadWithValues() {
			SimpleQueryFilter filter = valToCol(values("a"), "==", COL, PixelDataType.CONST_STRING);

			assertTrue(filter.addInstanceFilters(valToCol(values("b"), "==", COL, PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("a", "b"), filter.getLComparison().getValue());
		}

		@Test
		void appendsAcrossMirroredFilterSides() {
			SimpleQueryFilter colFirst = colToVal(COL, "==", values("a"), PixelDataType.CONST_STRING);
			assertTrue(colFirst.addInstanceFilters(valToCol(values("b"), "==", COL, PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("a", "b"), colFirst.getRComparison().getValue());

			SimpleQueryFilter valuesFirst = valToCol(values("a"), "==", COL, PixelDataType.CONST_STRING);
			assertTrue(valuesFirst.addInstanceFilters(colToVal(COL, "==", values("b"), PixelDataType.CONST_STRING)));
			assertEquals(Arrays.asList("a", "b"), valuesFirst.getLComparison().getValue());
		}

		@Test
		void leavesFiltersThatAreNotAboutColumnsAndValuesAlone() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			assertFalse(filter.addInstanceFilters(colToVal("==", "a")));
		}
	}

	@Nested
	class EmptyFilterValues {

		@Test
		void reportsEmptyWhenTheValueListIsEmpty() {
			assertTrue(colToVal(COL, "==", values(), PixelDataType.CONST_STRING).isEmptyFilterValues());
			assertTrue(valToCol(values(), "==", COL, PixelDataType.CONST_STRING).isEmptyFilterValues());
		}

		@Test
		void reportsNotEmptyWhenValuesArePresent() {
			assertFalse(colToVal(COL, "==", values("a"), PixelDataType.CONST_STRING).isEmptyFilterValues());
			assertFalse(valToCol(values("a"), "==", COL, PixelDataType.CONST_STRING).isEmptyFilterValues());
		}

		@Test
		void reportsNotEmptyForScalarValues() {
			assertFalse(colToVal("==", "a").isEmptyFilterValues());
		}

		@Test
		void reportsNotEmptyForFiltersWithoutValues() {
			assertFalse(SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL).isEmptyFilterValues());
		}
	}

	@Nested
	class Equality {

		@Test
		void matchesWhenComparatorAndBothSidesMatch() {
			assertTrue(colToVal("==", "a").equals(colToVal("==", "a")));
		}

		@Test
		void doesNotMatchWhenTheComparatorDiffers() {
			assertFalse(colToVal("==", "a").equals(colToVal("?like", "a")));
		}

		@Test
		void doesNotMatchWhenTheLeftSideDiffers() {
			SimpleQueryFilter filter = colToVal(COL, "==", "a", PixelDataType.CONST_STRING);
			SimpleQueryFilter other = colToVal(OTHER_COL, "==", "a", PixelDataType.CONST_STRING);

			assertFalse(filter.equals(other));
		}

		@Test
		void doesNotMatchWhenTheRightSideDiffers() {
			assertFalse(colToVal("==", "a").equals(colToVal("==", "b")));
		}

		@Test
		void doesNotMatchOtherKindsOfObjects() {
			assertFalse(colToVal("==", "a").equals("TABLE__COLUMN == a"));
			assertFalse(colToVal("==", "a").equals(null));
		}
	}

	@Nested
	class Copying {

		@Test
		void producesAnIndependentButEqualFilter() {
			SimpleQueryFilter filter = colToVal(COL, "==", values("a", "b"), PixelDataType.CONST_STRING);
			SimpleQueryFilter copy = (SimpleQueryFilter) filter.copy();

			assertNotSame(filter, copy);
			assertEquals("==", copy.getComparator());
			assertEquals(FILTER_TYPE.COL_TO_VALUES, copy.getSimpleFilterType());
			assertEquals(COL, ((QueryColumnSelector) copy.getLComparison().getValue()).getQueryStructName());
			assertEquals(Arrays.asList("a", "b"), new ArrayList<>((List<?>) copy.getRComparison().getValue()));
		}

		@Test
		void doesNotShareValuesWithTheOriginal() {
			SimpleQueryFilter filter = colToVal(COL, "==", values("a", "b"), PixelDataType.CONST_STRING);
			SimpleQueryFilter copy = (SimpleQueryFilter) filter.copy();

			copy.subtractInstanceFilters(colToVal(COL, "==", values("a"), PixelDataType.CONST_STRING));

			assertEquals(Arrays.asList("a", "b"), filter.getRComparison().getValue());
			assertEquals(Arrays.asList("b"), copy.getRComparison().getValue());
		}
	}

	@Nested
	class ColumnDiscovery {

		@Test
		void findsTheColumnByAliasOrByQueryStructName() {
			SimpleQueryFilter filter = colToVal("==", "a");

			assertTrue(filter.containsColumn("COLUMN"));
			assertTrue(filter.containsColumn(COL));
			assertFalse(filter.containsColumn("SOMETHING_ELSE"));
		}

		@Test
		void looksAtBothSidesOfTheComparison() {
			SimpleQueryFilter valuesFirst = valToCol("a", "==", COL, PixelDataType.CONST_STRING);

			assertTrue(valuesFirst.containsColumn(COL));
		}

		@Test
		void reportsNoColumnsWhenNeitherSideIsAColumn() {
			SimpleQueryFilter filter = new SimpleQueryFilter(new NounMetadata(1, PixelDataType.CONST_INT), "==",
					new NounMetadata(2, PixelDataType.CONST_INT));

			assertFalse(filter.containsColumn("COLUMN"));
			assertTrue(filter.getAllUsedColumns().isEmpty());
			assertTrue(filter.getAllQueryColumns().isEmpty());
			assertTrue(filter.getAllQueryStructNames().isEmpty());
			assertTrue(filter.getAllUsedTables().isEmpty());
		}

		@Test
		void collectsAliasesFromBothSides() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			assertEquals(Set.of("COLUMN", "FIELD"), filter.getAllUsedColumns());
		}

		@Test
		void collectsQueryStructNamesFromBothSides() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			assertEquals(Set.of(COL, OTHER_COL), filter.getAllQueryStructNames());
		}

		@Test
		void collectsTablesFromBothSides() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			assertEquals(Set.of("TABLE", "OTHER"), filter.getAllUsedTables());
		}

		@Test
		void collectsTheUnderlyingSelectors() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			List<QueryColumnSelector> selectors = filter.getAllQueryColumns();

			assertEquals(2, selectors.size());
			assertEquals(COL, selectors.get(0).getQueryStructName());
			assertEquals(OTHER_COL, selectors.get(1).getQueryStructName());
		}
	}

	@Nested
	class SimpleFormat {

		@Test
		void describesAColumnToValuesFilterAsNestedMaps() {
			SimpleQueryFilter filter = colToVal(COL, "==", values("a", "b"), PixelDataType.CONST_STRING);

			Map<String, Object> format = asMap(filter.getSimpleFormat());

			assertEquals(QUERY_FILTER_TYPE.SIMPLE, format.get("filterType"));
			assertEquals("==", format.get("comparator"));

			Map<String, Object> left = asMap(format.get("left"));
			assertEquals(PixelDataType.COLUMN, left.get("type"));
			assertEquals(COL, left.get("value"));

			Map<String, Object> right = asMap(format.get("right"));
			assertEquals(PixelDataType.CONST_STRING, right.get("type"));
			assertEquals(Arrays.asList("a", "b"), right.get("value"));
		}

		@Test
		void describesBothColumnsWhenComparingColumnToColumn() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			Map<String, Object> format = asMap(filter.getSimpleFormat());

			assertEquals(COL, asMap(format.get("left")).get("value"));
			assertEquals(OTHER_COL, asMap(format.get("right")).get("value"));
		}

		@Test
		void describesValuesOnTheLeftSide() {
			SimpleQueryFilter filter = valToCol("a", "==", COL, PixelDataType.CONST_STRING);

			Map<String, Object> format = asMap(filter.getSimpleFormat());

			assertEquals(PixelDataType.CONST_STRING, asMap(format.get("left")).get("type"));
			assertEquals("a", asMap(format.get("left")).get("value"));
			assertEquals(COL, asMap(format.get("right")).get("value"));
		}
	}

	@Nested
	class StringRepresentation {

		@Test
		void rendersAScalarColumnToValueFilter() {
			assertEquals("TABLE__COLUMN == a", colToVal("==", "a").getStringRepresentation());
		}

		@Test
		void rendersASingleValueListWithoutBrackets() {
			SimpleQueryFilter filter = colToVal(COL, "==", values("a"), PixelDataType.CONST_STRING);

			assertEquals("TABLE__COLUMN == a", filter.getStringRepresentation());
		}

		@Test
		void rendersAShortValueListInFull() {
			SimpleQueryFilter filter = colToVal(COL, "==", values("a", "b", "c"), PixelDataType.CONST_STRING);

			assertEquals("TABLE__COLUMN == [a, b, c ]", filter.getStringRepresentation());
		}

		@Test
		void truncatesALongValueList() {
			SimpleQueryFilter filter = colToVal(COL, "==", values(1, 2, 3, 4, 5, 6, 7), PixelDataType.CONST_INT);

			assertEquals("TABLE__COLUMN == [1, 2, 3, 4, 5, ... ]", filter.getStringRepresentation());
		}

		@Test
		void flipsTheComparatorWhenTheValuesComeFirst() {
			SimpleQueryFilter scalar = valToCol(5, ">", COL, PixelDataType.CONST_INT);
			assertEquals("TABLE__COLUMN < 5", scalar.getStringRepresentation());

			SimpleQueryFilter singleValueList = valToCol(values(5), ">", COL, PixelDataType.CONST_INT);
			assertEquals("TABLE__COLUMN < 5", singleValueList.getStringRepresentation());
		}

		@Test
		void truncatesALongValueListWhenTheValuesComeFirst() {
			SimpleQueryFilter filter = valToCol(values(1, 2, 3, 4, 5, 6, 7), ">", COL, PixelDataType.CONST_INT);

			assertEquals("TABLE__COLUMN < [1, 2, 3, 4, 5, ... ]", filter.getStringRepresentation());
		}

		@Test
		void rendersAShortValueListWhenTheValuesComeFirst() {
			SimpleQueryFilter filter = valToCol(values(1, 2, 3), ">", COL, PixelDataType.CONST_INT);

			assertEquals("TABLE__COLUMN < [1, 2, 3 ]", filter.getStringRepresentation());
		}

		@Test
		void rendersBothColumnsWhenComparingColumnToColumn() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			assertEquals("TABLE__COLUMN == OTHER__FIELD", filter.getStringRepresentation());
		}

		@Test
		void fallsBackToTheRawValuesForEverythingElse() {
			SimpleQueryFilter filter = new SimpleQueryFilter(new NounMetadata(1, PixelDataType.CONST_INT), "==",
					new NounMetadata(2, PixelDataType.CONST_INT));

			assertEquals("1 == 2", filter.getStringRepresentation());
		}
	}

	@Nested
	class RequireOrBetweenFilters {

		@Test
		void requiresOrWhenTheTwoRangesDoNotOverlap() {
			assertTrue(SimpleQueryFilter.requireOrBetweenFilters(numeric(">", 10), numeric("<", 5)));
			assertTrue(SimpleQueryFilter.requireOrBetweenFilters(numeric(">=", 10), numeric("<", 5)));
			assertTrue(SimpleQueryFilter.requireOrBetweenFilters(numeric(">=", 10), numeric("<=", 5)));
			assertTrue(SimpleQueryFilter.requireOrBetweenFilters(numeric("<", 5), numeric(">", 10)));
			assertTrue(SimpleQueryFilter.requireOrBetweenFilters(numeric("<=", 5), numeric(">", 10)));
			assertTrue(SimpleQueryFilter.requireOrBetweenFilters(numeric("<=", 5), numeric(">=", 10)));
		}

		@Test
		void doesNotRequireOrWhenTheTwoRangesOverlap() {
			assertFalse(SimpleQueryFilter.requireOrBetweenFilters(numeric(">", 5), numeric("<", 10)));
			assertFalse(SimpleQueryFilter.requireOrBetweenFilters(numeric(">", 5), numeric("<=", 10)));
			assertFalse(SimpleQueryFilter.requireOrBetweenFilters(numeric(">=", 5), numeric("<", 10)));
			assertFalse(SimpleQueryFilter.requireOrBetweenFilters(numeric(">=", 5), numeric("<=", 10)));
			assertFalse(SimpleQueryFilter.requireOrBetweenFilters(numeric("<", 10), numeric(">", 5)));
			assertFalse(SimpleQueryFilter.requireOrBetweenFilters(numeric("<=", 10), numeric(">=", 5)));
		}

		@Test
		void readsTheBoundOutOfAList() {
			SimpleQueryFilter lower = colToVal(COL, ">", values(10), PixelDataType.CONST_INT);
			SimpleQueryFilter upper = colToVal(COL, "<", values(5), PixelDataType.CONST_INT);

			assertTrue(SimpleQueryFilter.requireOrBetweenFilters(lower, upper));
		}

		@Test
		void ignoresNonNumericComparators() {
			assertFalse(SimpleQueryFilter.requireOrBetweenFilters(colToVal("==", "a"), colToVal("!=", "b")));
			assertFalse(SimpleQueryFilter.requireOrBetweenFilters(colToVal("==", "a"), numeric("<", 5)));
		}

		@Test
		void ignoresComparatorsPointingTheSameDirection() {
			assertFalse(SimpleQueryFilter.requireOrBetweenFilters(numeric(">", 5), numeric(">=", 10)));
			assertFalse(SimpleQueryFilter.requireOrBetweenFilters(numeric("<", 5), numeric("<=", 10)));
		}

		@Test
		void ignoresFiltersAboutDifferentColumns() {
			SimpleQueryFilter lower = colToVal(COL, ">", 10, PixelDataType.CONST_INT);
			SimpleQueryFilter upper = colToVal(OTHER_COL, "<", 5, PixelDataType.CONST_INT);

			assertFalse(SimpleQueryFilter.requireOrBetweenFilters(lower, upper));
		}

		@Test
		void ignoresFiltersThatAreNotAboutColumnsAndValues() {
			SimpleQueryFilter colToCol = SimpleQueryFilter.makeColToColFilter(COL, ">", OTHER_COL);

			assertFalse(SimpleQueryFilter.requireOrBetweenFilters(colToCol, numeric("<", 5)));
			assertFalse(SimpleQueryFilter.requireOrBetweenFilters(numeric(">", 10), colToCol));
		}

		@Test
		void comparesTheColumnRegardlessOfWhichSideHoldsIt() {
			SimpleQueryFilter lower = colToVal(COL, ">", 10, PixelDataType.CONST_INT);
			SimpleQueryFilter upper = valToCol(5, "<", COL, PixelDataType.CONST_INT);

			assertTrue(SimpleQueryFilter.requireOrBetweenFilters(lower, upper));
		}
	}

	@Nested
	class NullValueDetection {

		@Test
		void detectsNullInAValueList() {
			assertTrue(SimpleQueryFilter
					.colValuesContainsNull(colToVal(COL, "==", values("a", null), PixelDataType.CONST_STRING)));
			assertTrue(SimpleQueryFilter
					.colValuesContainsNull(valToCol(values("a", null), "==", COL, PixelDataType.CONST_STRING)));
		}

		@Test
		void detectsAScalarNull() {
			assertTrue(SimpleQueryFilter.colValuesContainsNull(colToVal(COL, "==", null, PixelDataType.CONST_STRING)));
			assertTrue(SimpleQueryFilter.colValuesContainsNull(valToCol(null, "==", COL, PixelDataType.CONST_STRING)));
		}

		@Test
		void reportsNoNullWhenEveryValueIsPresent() {
			assertFalse(SimpleQueryFilter
					.colValuesContainsNull(colToVal(COL, "==", values("a"), PixelDataType.CONST_STRING)));
			assertFalse(SimpleQueryFilter.colValuesContainsNull(colToVal("==", "a")));
		}

		@Test
		void reportsNoNullForFiltersWithoutValues() {
			assertFalse(SimpleQueryFilter
					.colValuesContainsNull(SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL)));
		}
	}

	@Nested
	class SelectAllDetection {

		@Test
		void treatsAnEmptyLikeAsSelectAll() {
			assertTrue(SimpleQueryFilter.isSelectAll(colToVal("?like", "")));
			assertTrue(SimpleQueryFilter.isSelectAll(colToVal(COL, "?like", values(""), PixelDataType.CONST_STRING)));
			assertTrue(SimpleQueryFilter.isSelectAll(valToCol(values(""), "?like", COL, PixelDataType.CONST_STRING)));
			assertTrue(SimpleQueryFilter.isSelectAll(valToCol("", "?like", COL, PixelDataType.CONST_STRING)));
		}

		@Test
		void doesNotTreatARealSearchTermAsSelectAll() {
			assertFalse(SimpleQueryFilter.isSelectAll(colToVal("?like", "warn")));
			assertFalse(SimpleQueryFilter
					.isSelectAll(colToVal(COL, "?like", values("", "warn"), PixelDataType.CONST_STRING)));
		}

		@Test
		void doesNotTreatOtherComparatorsAsSelectAll() {
			assertFalse(SimpleQueryFilter.isSelectAll(colToVal("==", "")));
			assertFalse(SimpleQueryFilter.isSelectAll(colToVal("?nlike", "")));
		}

		@Test
		void treatsAnEmptyNotLikeAsUnselectAll() {
			assertTrue(SimpleQueryFilter.isUnselectAll(colToVal("?nlike", "")));
			assertTrue(
					SimpleQueryFilter.isUnselectAll(colToVal(COL, "?nlike", values(""), PixelDataType.CONST_STRING)));
			assertTrue(
					SimpleQueryFilter.isUnselectAll(valToCol(values(""), "?nlike", COL, PixelDataType.CONST_STRING)));
			assertTrue(SimpleQueryFilter.isUnselectAll(valToCol("", "?nlike", COL, PixelDataType.CONST_STRING)));
		}

		@Test
		void doesNotTreatARealSearchTermAsUnselectAll() {
			assertFalse(SimpleQueryFilter.isUnselectAll(colToVal("?nlike", "warn")));
		}

		@Test
		void doesNotTreatOtherComparatorsAsUnselectAll() {
			assertFalse(SimpleQueryFilter.isUnselectAll(colToVal("!=", "")));
			assertFalse(SimpleQueryFilter.isUnselectAll(colToVal("?like", "")));
		}
	}

	@Nested
	class StaticFactories {

		@Test
		void buildsAColumnToValuesFilterDefaultingToStringValues() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToValFilter(COL, "==", "a");

			assertEquals(FILTER_TYPE.COL_TO_VALUES, filter.getSimpleFilterType());
			assertEquals(COL, ((QueryColumnSelector) filter.getLComparison().getValue()).getQueryStructName());
			assertEquals(PixelDataType.CONST_STRING, filter.getRComparison().getNounType());
			assertEquals("a", filter.getRComparison().getValue());
		}

		@Test
		void buildsAColumnToValuesFilterWithAnExplicitValueType() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToValFilter(COL, ">", 5, PixelDataType.CONST_INT);

			assertEquals(PixelDataType.CONST_INT, filter.getRComparison().getNounType());
			assertEquals(5, filter.getRComparison().getValue());
		}

		@Test
		void buildsAColumnToValuesFilterFromAnExistingSelector() {
			QueryColumnSelector selector = new QueryColumnSelector(COL);
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToValFilter(selector, "==", "a",
					PixelDataType.CONST_STRING);

			assertEquals(FILTER_TYPE.COL_TO_VALUES, filter.getSimpleFilterType());
			assertSame(selector, filter.getLComparison().getValue());
		}

		@Test
		void buildsAColumnToColumnFilterFromQueryStructNames() {
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToColFilter(COL, "==", OTHER_COL);

			assertEquals(FILTER_TYPE.COL_TO_COL, filter.getSimpleFilterType());
			assertEquals(COL, ((QueryColumnSelector) filter.getLComparison().getValue()).getQueryStructName());
			assertEquals(OTHER_COL, ((QueryColumnSelector) filter.getRComparison().getValue()).getQueryStructName());
		}

		@Test
		void buildsAColumnToColumnFilterFromExistingSelectors() {
			QueryColumnSelector left = new QueryColumnSelector(COL);
			QueryColumnSelector right = new QueryColumnSelector(OTHER_COL);
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToColFilter(left, "==", right);

			assertEquals(FILTER_TYPE.COL_TO_COL, filter.getSimpleFilterType());
			assertSame(left, filter.getLComparison().getValue());
			assertSame(right, filter.getRComparison().getValue());
		}

		@Test
		void buildsAColumnToSubQueryFilterFromAQueryStructName() {
			SelectQueryStruct subQuery = new SelectQueryStruct();
			SimpleQueryFilter filter = SimpleQueryFilter.makeColToSubQuery(COL, "==", subQuery);

			assertEquals(FILTER_TYPE.COL_TO_QUERY, filter.getSimpleFilterType());
			assertEquals(COL, ((QueryColumnSelector) filter.getLComparison().getValue()).getQueryStructName());
			assertSame(subQuery, filter.getRComparison().getValue());
		}

		@Test
		void buildsAColumnToSubQueryFilterFromAnExistingSelector() {
			QueryColumnSelector selector = new QueryColumnSelector(COL);
			SelectQueryStruct subQuery = new SelectQueryStruct();
			SimpleQueryFilter filter = SimpleQueryFilter.makeQuerySelectorToSubQuery(selector, "==", subQuery);

			assertEquals(FILTER_TYPE.COL_TO_QUERY, filter.getSimpleFilterType());
			assertSame(selector, filter.getLComparison().getValue());
			assertSame(subQuery, filter.getRComparison().getValue());
		}
	}

	/*
	 * Helpers
	 */

	/** A column to values filter on {@link #COL} holding a single string. */
	private static SimpleQueryFilter colToVal(String comparator, Object value) {
		return colToVal(COL, comparator, value, PixelDataType.CONST_STRING);
	}

	private static SimpleQueryFilter colToVal(String colQs, String comparator, Object value, PixelDataType type) {
		return new SimpleQueryFilter(new NounMetadata(new QueryColumnSelector(colQs), PixelDataType.COLUMN), comparator,
				new NounMetadata(value, type));
	}

	/** The mirror image of {@link #colToVal}, with the values on the left. */
	private static SimpleQueryFilter valToCol(Object value, String comparator, String colQs, PixelDataType type) {
		return new SimpleQueryFilter(new NounMetadata(value, type), comparator,
				new NounMetadata(new QueryColumnSelector(colQs), PixelDataType.COLUMN));
	}

	/** A numeric column to value filter on the shared {@link #COL}. */
	private static SimpleQueryFilter numeric(String comparator, int value) {
		return colToVal(COL, comparator, value, PixelDataType.CONST_INT);
	}

	/**
	 * A mutable value list, since the filter operations remove from it in place.
	 */
	private static List<Object> values(Object... items) {
		return new ArrayList<>(Arrays.asList(items));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> asMap(Object obj) {
		return (Map<String, Object>) obj;
	}
}
