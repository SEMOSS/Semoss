package prerna.rdf.util;

import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;

public class MyTupleQuery extends SailTupleQuery {

	public MyTupleQuery(ParsedTupleQuery query, SailRepositoryConnection sc) {
		super(query, sc);
	}
}
