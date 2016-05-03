package prerna.sablecc;

import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;

import prerna.ds.TinkerFrame;
import prerna.ds.H2.TinkerH2Frame;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.sablecc.lexer.Lexer;
import prerna.sablecc.node.Start;
import prerna.sablecc.parser.Parser;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class Compiler
{
 public static void main(String[] arguments)
 {
	String workingDir = System.getProperty("user.dir");
	String propFile = workingDir + "/RDF_Map.prop";
	DIHelper.getInstance().loadCoreProp(propFile);
	 
	 
  try
  {


   // Create a Parser instance.
   Parser p =
    new Parser(
    new Lexer(
    new PushbackReader(
    //new InputStreamReader(new StringBufferInputStream("5;")), 1024)));
  // new InputStreamReader(new StringBufferInputStream(" 1 + 2 * (3 + 5); ")), 1024)));
   //new InputStreamReader(new StringBufferInputStream("j:{a b c d };")), 1024)));
    new InputStreamReader(new StringBufferInputStream(""
    												//+ "(3.0 + 4) * c:t;"
    												//+ "(3 + 4, [c:bp, c:ab]);"
    												//+ "col.import([c:col1,c:col2], ([c:col1, inner.join, c:col2])) . [[\"a\",\"b\"][2,3]];"
    												//+ ";" //; , [[2,1.0][\"a hello world would work absolutely fine too __ ab\",\"b\"]]);"
//    												+ "col.add(c:newCol, (4 - c:Capability) + c:Activity); "
//    												+ "api:Movie_DB.query([c:Title__Title, c:Title__MovieBudget, c:Studio__Studio], (c:Studio__Studio =[\"WB\",\"Fox\"]), ([c:Title__Title,  inner.join , c:Studio__Studio])); "
//    												+ "col.add(c:test, api:UpdatedRDBMSMovies.query([c:Title__Title, c:Title__MovieBudget, c:Studio__Studio], (c:Studio__Studio =[\"WB\",\"Fox\"]), ([c:Title__Title,  right.outer.join , c:Studio__Title_FK])));"
//    												+ "col.add(c:test, api:UpdatedRDBMSMovies.query([c:Title__Title, c:Title__MovieBudget, c:Studio__Studio], (c:Studio__Studio =[\"WB\",\"Fox\"]), ([c:Studio__Title_FK,  right.outer.join , c:Title__Title])));"
//    												+ "data.import(api:MovieDatabase.query([c:Title, c:Studio], (c:Studio =[\"WB\",\"Fox\"]), ([c:Title,  inner.join , c:Studio])));"
//    												+ "data.import(api:Movie_Results.query([c:Title, c:Director], ([c:Title,  inner.join , c:Director])), ([c:Title, inner.join, c:Title]));"
//    												+ "data.import(api:Movie_Results.query([c:Title, c:Producer], ([c:Title,  inner.join , c:Producer])), ([c:Title, inner.join, c:Title]));"
//    												+ "data.import(api:Movie_Results.query([c:Title, c:Year], ([c:Title,  inner.join , c:Year])), ([c:Title, inner.join, c:Title]));"
//    												+ "data.import(api:Documents\\something.csv.query([c:Title, c:Year]), ([c:Title, inner.join, c:Title]));"

//    												+ "data.import(api:Movie_DB.query([c:Title, c:Title__MovieBudget, c:Studio], (c:Studio =[\"WB\",\"Fox\"]), ([c:Title,  inner.join , c:Studio])));"
//    												+ "panel[0].viz(Bar, [c:Title, c:Studio]);"
    												
    												+ "data.import(api:csvFile.query([c:Title, c:Nominated, c:Studio], (c:Title = ['12 Years a Slave'], c:Nominated = ['Y']), {'file':'C:\\Users\\bisutton\\Desktop\\Movie Data.csv'}));"
    												+ "data.import(api:csvFile.query([c:Title, c:Actor], {'file':'C:\\Users\\bisutton\\Desktop\\Best Actor.csv'}), ([c:Title, inner.join, c:Title]));"

//    												+ "data.remove(api:MovieDatabase.query([c:Title, c:Title__MovieBudget, c:Studio], (c:Studio =[\"WB\",\"Fox\"]), ([c:Title,  inner.join , c:Studio])));"
//    												+"panel[\"123\"].comment(\"this looks super important\", group0, coordinate, \"a\");"
//    												+"col.filter(c:Studio =[\"WB\",\"Fox\",\"Lionsgate\"]) ;"
//    												+"col.unfilter(c:Studio);"
//    												+ "col.add(c:test, api:Movie_DB.query([c:Title__Title, c:Title__MovieBudget, c:Studio__Studio], (c:Studio__Studio =[\"WB\",\"Fox\"]), ([c:Studio__Studio,  right.outer.join , c:Title__Title])));"
    												//+ "api:TAP_CORE.query([c:Title__title, c:Title__movie_budget], (c:Title__Title =[\"a\",\"b\"], c:Title__Title > [\"c\", \"d\"]), ([c:Title__Title,  inner.join , c:Studio__Studio] ,[c:Title__Title,  outer.join , c:Studio__Studio_FK]) ); "    												//+ "(m:Sum([(2 * 3) * (c:Capability * c:Activity) + c:Activity], [c:bp, c:ab]));"
    												//+ "3 + (m:Sum([(2 * 3) * (c:Capability * c:Activity) + c:Activity], [c:bp, c:ab]));"
    												// m:Sum(X) >= m:Sum(Capability)
    												//+ "[a,b,c,d,e,f,e];"
    												// can I use the calculation once again in something else ?
    												// I almost need to break everytime I see a math function
    												// but how do I tell groovy not to use it anymore ?
    												// i.e. the value is there so just take that
    												// I bet I need to replace the equation so that I can do it
    												// in reality I only need to reduce when I need to < - that is profound prabhu
    												// Ok.. I bet what I am saying is I need to reduce only when I see the pattern closing and then I need to replace it in the expression
    												
    												
    												

//    												+"(12 + (4 - 8)) * (15 / 5) + 5;"
   													//+ "v:abc = (c:col2 * (2 *4)); "
    												/*+ "set:(c:newCol, (2 * 3) + 4); "
   													+ "code:{System.out.println('yo');};"
    												+ "pivot:(c:col1, c:col2);"
   													+ "join:(c:newColumn, c:existingcol, c:existingcol2);"
   													+ "del:(c:colToRemove);"
   													+ "rm:(c:colToRemove, c:anotherColToRemove);"*/
   													//+ "jc"
   													+ "")), 1024)));
// new InputStreamReader(System.in), 1024)));

   // Parse the input.
   Start tree = p.parse();


   // Apply the translation.
   tree.apply(new Translation2(new TinkerFrame(), new PKQLRunner()));
  }
  catch(Exception e)
  {
   System.out.println(e.getMessage());
   e.printStackTrace();
  }
 }
}