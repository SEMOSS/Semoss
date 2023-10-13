package prerna.reactor.json;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Vector;

public class GreedyJsonProcessor extends GreedyJsonReactor {
	
	// couple of things I need to take care of here
	// column to json name
	Connection conn = null;
	
	// keeps columns to keys
	public Hashtable <String, String> colKeyHash = new Hashtable<String, String>();
	
	// keeps keys to columns
	public Hashtable <String, String> keyColHash = new Hashtable<String, String>();
	
	public Connection getConnection()
	{
		return conn;
	}
	
	public void setConnection(Connection conn)
	{
		this.conn = conn;
	}
	
	// the idea here is to take the sql thaat is there
	// print it out
	// in the right location
	public Hashtable getSqlOutput()
	{
		boolean hasError = isError();
		Hashtable mainTable = new Hashtable();
		
		if(hasError)
		{
			mainTable = store.getDataHash();
			
			Vector <Hashtable> allHashes = new Vector<Hashtable>();
			allHashes.add(mainTable);
			
			while(allHashes.size() > 0)
			{
				// so I am in this hash at this point
				// the first hash is not a sql.. I dont need to do anything
				// I only pick the childs and add
				// if the child hash has a SQL
				// I need to execute the sql and then replace the key on parent hash
				// but I dont even need to do that
				// I add the childs of this child has
				// I empty this hash
				// and I replace this hash with whatever value jsut came back
				// but this works for hash but not if the result is a vector
				// need some way to go back to the parent on it
				// remove the first one
				Hashtable parentHash = allHashes.remove(0);
				// if it has child hashes
				// this means you need to process the child
				if(parentHash.containsKey(CHILDS))
				{
					Vector <String> childs = (Vector<String>)parentHash.get(CHILDS);
					for(int childIndex = 0;childIndex < childs.size();childIndex++)
					{
						String childName = childs.get(childIndex);
						
						// seems to come 2 times need to see why
						// now way this wont come not sure why this exists but ok
						if(parentHash.containsKey(childName))
						{
							// need to make a check to see if this is an array
							// if so leave it
							if(parentHash.get(childName) instanceof Hashtable)
							{
								Hashtable thisChildHash = (Hashtable)parentHash.get(childName);
								// we have to take this SQL
								// if the child hash has it
								// and then convert it into a hash
								// but.. we also need to add the child hash to it
								// and then add the hash to allhashes
								
								// see if there is SQL for you to process SQL
								if(thisChildHash.containsKey(SQL))
								{
									// start or stop transaction if you choose to
									if(thisChildHash.containsKey(TXN))
										System.out.println("We will start transaction here");
									
									// sql part comes here
									// one thing we need to think about is what if the output is not a hashtable
									Object sqlOutput = executeQuery(thisChildHash.get(SQL) +"");
									if(thisChildHash.containsKey(CHILDS))
									{
										Hashtable childOutput = null;
										if(sqlOutput instanceof Hashtable)
											childOutput = (Hashtable)sqlOutput;
										else
										{
											childOutput = new Hashtable();
											childOutput.put("OUTPUT", sqlOutput);
										}
										// merge it now
										childOutput = mergeHash(thisChildHash, (Hashtable)sqlOutput);
										// replace the sql output as well;
										sqlOutput = childOutput;
										// since we add this child hash for further processing
										// change it as well
										thisChildHash = childOutput;
										
										// and add it.. else there is no reason to process this child hash
										allHashes.add(thisChildHash);							
									}
									// add this back
									// if there are no childs, this child will get fully replaced with whatever out put comes from sql
									parentHash.put(childName, sqlOutput);
									
									if(thisChildHash.containsKey(TXN))
										System.out.println("We will end transaction here");
								}
							}
							else
								System.out.println("Nothing to do here");
							//thisHash.remove(childs.get(childIndex));
						}
					}
					parentHash.remove(CHILDS);
				}
				parentHash.remove("all");
			}
		}		
		return mainTable;
	}

	public Hashtable mergeHash(Hashtable sourceHash, Hashtable targetHash)
	{
		// gets the childs
		if(sourceHash.containsKey(CHILDS))
		{
			Vector <String> childs = (Vector<String>)sourceHash.get(CHILDS);			
			targetHash.put(CHILDS, childs);
			for(int childIndex = 0;childIndex < childs.size();childIndex++)
				targetHash.put(childs.get(childIndex), sourceHash.get(childs.get(childIndex)));
		}
		return targetHash;
	}
	
	public Object executeQuery(String query)
	{
		StringBuffer retBuffer = new StringBuffer();
		
		// execute the query
		// push everything into a hashtable
		// serialize the hashtable
		
		// there are a couple of things to take care of
		// when I get the column label
		// I need to find if there is a key for this column and if so use that
		
		Hashtable finalHash = null;
		Vector finalVector = null;
		try (Statement stmt = conn.createStatement()){
			ResultSet rs = stmt.executeQuery(query);
			ResultSetMetaData rsmd = rs.getMetaData();
			int colCount = rsmd.getColumnCount();
			Vector <String> outCols = new Vector<String>();
			Vector <String> aliases = new Vector<String>();
			for(int colIndex = 1;colIndex <= colCount;colIndex++)
			{
				String colLabel = rsmd.getColumnLabel(colIndex);
				outCols.add(colLabel);
				if(colKeyHash.containsKey(colLabel))
					aliases.add(colKeyHash.get(colLabel));
				else
					aliases.add(colLabel); // leave it to what it is
			}
			
			finalHash = null; 
			int count = 0;
			finalVector = null;
			while(rs.next())
			{
				if(count > 0)
				{
					// we already have a piece of data so this needs to be added to the vector
					finalVector = new Vector();
					finalVector.add(finalHash);
				}
				finalHash = new Hashtable();
				
				for(int colIndex = 0;colIndex < outCols.size();colIndex++)
					finalHash.put(aliases.elementAt(colIndex), rs.getObject(outCols.get(colIndex)));
				count++;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(finalVector != null) // the vector was filled up
			return finalVector;
			//retBuffer.append(gson.toJson(finalVector));
		// else this is just a hash
		else if(finalHash != null)
			return finalHash;

		// you are all done.. return the buffer back
		// return an empty string if both of these are null;
		return "";
	}
	


}
