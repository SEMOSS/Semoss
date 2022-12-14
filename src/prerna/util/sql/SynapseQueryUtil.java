package prerna.util.sql;

public class SynapseQueryUtil extends MicrosoftSqlServerQueryUtil {

	SynapseQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.SYNAPSE);
	}
	
	SynapseQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.SYNAPSE);	}

	@Override
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {
		
		if(limit > 0) {
			String strquery = query.toString();
			if(strquery.startsWith("SELECT DISTINCT")){
			strquery=strquery.replaceFirst("SELECT DISTINCT", "SELECT DISTINCT TOP " + limit + " ");
			} else {
				strquery=strquery.replaceFirst("SELECT", "SELECT TOP " + limit + " ");
			}
			query = new StringBuilder();
			query.append(strquery);
		}
		
		//TODO there is no offset for now
//		if(offset > 0) {
//			query = query.append(" OFFSET "+offset);
//		}
		return query;
	}
	
	@Override
	public StringBuffer addLimitOffsetToQuery(StringBuffer query, long limit, long offset) {

		if(limit > 0) {
			String strquery = query.toString();
			if(strquery.startsWith("SELECT DISTINCT")){
				strquery=strquery.replaceFirst("SELECT DISTINCT", "SELECT DISTINCT TOP " + limit + " ");
				} else {
					strquery=strquery.replaceFirst("SELECT", "SELECT TOP " + limit + " ");
							query = new StringBuffer();
				}
			query.append(strquery);
		}
		
		//TODO there is no offset for now
//		if(offset > 0) {
//			query = query.append(" OFFSET "+offset);
//		}
		return query;
	}
	
//	//this creates the temp table to select top from the entire list of distinct selectors. 
//	//this is only used with distinct
//	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset, String tempTable) {
//
//		if(limit > 0) {
//			query=query.insert(0, "SELECT TOP " + limit + " * from (");
//			query=query.append(") as "+ tempTable);
//		}
//		
//		//TODO there is no offset for now
////		if(offset > 0) {
////			query = query.append(" OFFSET "+offset);
////		}
//		return query;
//	}

}
