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
package prerna.rdf.engine.wrappers;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Set;
import javax.xml.datatype.XMLGregorianCalendar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;
import prerna.om.ThreadStore;
import prerna.usertracking.UserQueryTrackingThread;
import prerna.util.Constants;
import prerna.util.Utility;

public class RawRDF4JSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

  private static final Logger classLogger =
      LogManager.getLogger(RawRDF4JSelectWrapper.class.getName());

  private final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");

  private TupleQueryResult tqr = null;

  @Override
  public void execute() throws Exception {
    tqr = (TupleQueryResult) engine.execQuery(query);
    // set the variables for future use
    setVariables();
  }

  @Override
  public boolean hasNext() {
    boolean retBool = false;
    try {
      retBool = tqr.hasNext();
      if (!retBool) {
        tqr.close();
      }
    } catch (QueryEvaluationException e) {
      classLogger.error(Constants.STACKTRACE, e);
    }
    return retBool;
  }

  @Override
  public IHeadersDataRow next() {
    try {
      // need to store both the clean and raw values
      Object[] cleanRow = new Object[numColumns];
      Object[] rawRow = new Object[numColumns];

      BindingSet bindSet = tqr.next();
      for (int colIndex = 0; colIndex < numColumns; colIndex++) {
        Value val = bindSet.getValue(rawHeaders[colIndex]);
        // raw value is the straight return from the binding set
        rawRow[colIndex] = val;
        // get the real value in the clean row
        cleanRow[colIndex] = getRealValue(val);
      }

      return new HeadersDataRow(headers, cleanRow, rawRow);
    } catch (QueryEvaluationException e) {
      classLogger.error(Constants.STACKTRACE, e);
    }

    return null;
  }

  private void setVariables() {
    try {
      // this makes the assumption that the query is constructed
      // using the logic within the SPARQL Query Builder

      // get the vars from the tuple result
      List<String> names = tqr.getBindingNames();
      numColumns = names.size();

      // what should be in physical names?
      // we technically need the concept and prop name
      // this is already what we have via the names binding
      // when it is created through query builder
      rawHeaders = names.toArray(new String[names.size()]);

      headers = new String[numColumns];
      for (int colIndex = 0; colIndex < numColumns; colIndex++) {
        // for the display, if we encounter a "__", we want to
        // split and get the second part of the string
        // that is the display for the column
        String columnLabel = names.get(colIndex);
        if (columnLabel.contains("__")) {
          String[] splitColAndTable = columnLabel.split("__");
          columnLabel = splitColAndTable[1];
        }
        headers[colIndex] = columnLabel;
      }
    } catch (QueryEvaluationException e) {
      classLogger.error(Constants.STACKTRACE, e);
    }
  }

  @Override
  public String[] getHeaders() {
    return headers;
  }

  private Object getRealValue(Object val) {
    if (val == null) {
      return null;
    }
    try {
      if (val instanceof Literal) {
        // use datatype if present to determine the type
        Literal lVal = (Literal) val;
        IRI lValDataType = lVal.getDatatype();
        if (lValDataType == null) {
          return lVal.getLabel();
        }

        // if string, return string
        if (QueryEvaluationUtil.isStringLiteral(lVal)) {
          return lVal.getLabel();
        }
        // if int
        else if (lValDataType.getLocalName().equalsIgnoreCase("integer")) {
          return new Integer(lVal.intValue());
        }
        // if double
        else if (lValDataType.getLocalName().equalsIgnoreCase("double")) {
          return new Double(lVal.doubleValue());
        }
        // if boolean
        else if (lValDataType.getLocalName().equalsIgnoreCase("boolean")) {
          return lVal.booleanValue();
        }
        // if datetime
        else if (lValDataType.getLocalName().equalsIgnoreCase("dateTime")) {
          try {
            LocalDateTime ldt =
                LocalDateTime.parse(lVal.calendarValue().toString(), DATE_FORMATTER);
            Date d = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
            SemossDate date =
                new SemossDate(d, "yyyy-MM-dd HH:mm:ss", ZoneId.of(Utility.getApplicationZoneId()));
            return date;
          } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
          }
          return null;
        }
        // if date
        else if (lValDataType.getLocalName().equalsIgnoreCase("date")) {
          XMLGregorianCalendar gCalendar = lVal.calendarValue();
          SemossDate date = new SemossDate(gCalendar.toGregorianCalendar().getTime(), "yyyy-MM-dd");
          return date;
        }
        // if float
        else if (lValDataType.getLocalName().equalsIgnoreCase("float")) {
          return new Double(lVal.floatValue());
        } else {
          classLogger.warn("Unknown data type returned from query = " + lValDataType);
          classLogger.warn("Unknown data type returned from query = " + lValDataType);
          classLogger.warn("Unknown data type returned from query = " + lValDataType);
          classLogger.warn("Unknown data type returned from query = " + lValDataType);
        }
      }
      // why would i get a jena here?? we are sesame wrapper..
      else if (val instanceof org.apache.jena.rdf.model.Literal) {
        classLogger.debug("Class is " + val.getClass());
        return new Double(((org.apache.jena.rdf.model.Literal) val).getDouble());
      }

      String value = val + "";
      return Utility.getInstanceName(value);
    } catch (RuntimeException ex) {
      classLogger.debug(ex);
    }

    return val;
  }

  @Override
  public SemossDataType[] getTypes() {
    if (this.types == null) {
      try {
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery parsedQuery = parser.parseQuery(query, null);

        CustomRdf4JSparqlAggregationParser aggregationVisitor =
            new CustomRdf4JSparqlAggregationParser();
        parsedQuery.getTupleExpr().visit(aggregationVisitor);
        Set<String> aggregationValues = aggregationVisitor.getValue();

        this.types = new SemossDataType[this.numColumns];
        for (int i = 0; i < this.numColumns; i++) {
          if (aggregationValues.contains(this.rawHeaders[i])) {
            this.types[i] = SemossDataType.DOUBLE;
          } else {
            this.types[i] = SemossDataType.STRING;
          }
        }
      } catch (Exception e) {
        classLogger.error(Constants.STACKTRACE, e);
        this.types = new SemossDataType[this.numColumns];
        for (int i = 0; i < this.numColumns; i++) {
          this.types[i] = SemossDataType.STRING;
        }
      }
    }
    return this.types;
  }

  @Override
  public void close() throws IOException {
    if (this.tqr != null) {
      try {
        tqr.close();
      } catch (QueryEvaluationException e) {
        classLogger.error(Constants.STACKTRACE, e);
        throw new IOException(e);
      }
    }
  }

  @Override
  public long getNumRows() throws Exception {
    if (this.numRows == 0) {
      User user = ThreadStore.getUser();
      UserQueryTrackingThread queryT = new UserQueryTrackingThread(user, this.engine.getEngineId());

      String query = "select (count(*) as ?count) where { " + this.query + "}";
      TupleQueryResult tqr = null;
      try {
        queryT.setQuery(query);
        queryT.setStartTimeNow();

        tqr = (TupleQueryResult) engine.execQuery(query);
        queryT.setEndTimeNow();
        if (tqr.hasNext()) {
          BindingSet bindSet = tqr.next();
          Object val = bindSet.getValue("count");
          Object cleanValue = getRealValue(val);
          if (cleanValue instanceof Number) {
            this.numRows = ((Number) cleanValue).longValue();
          }
        }
      } catch (QueryEvaluationException e) {
        queryT.setFailed();
        classLogger.error(Constants.STACKTRACE, e);
      } finally {
        try {
          tqr.close();
        } catch (QueryEvaluationException e) {
          classLogger.error(Constants.STACKTRACE, e);
        }

        new Thread(queryT).start();
      }
    }

    return this.numRows;
  }

  @Override
  public long getNumRecords() throws Exception {
    return getNumRows() * getHeaders().length;
  }

  @Override
  public void reset() throws Exception {
    close();
    execute();
  }

  @Override
  public boolean flushable() {
    return false;
  }

  @Override
  public String flush() {
    return null;
  }
}
