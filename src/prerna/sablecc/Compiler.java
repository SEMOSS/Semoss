package prerna.sablecc;

import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;

import prerna.sablecc.lexer.Lexer;
import prerna.sablecc.node.Start;
import prerna.sablecc.parser.Parser;

public class Compiler
{
 public static void main(String[] arguments)
 {
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
    												+ "col.add(c:newCol, (4 - c:Capability) + c:Activity); "
    												//+ "col.add(c:newCol, api:TAP_CORE.query([c:Title__title, c:Title__movie_budget], (c:Title__Title =[\"a\",\"b\"], c:Title__Title > [\"c\", \"d\"]), ([c:Title__Title,  inner.join , c:Studio__Studio] ,[c:Title__Title,  outer.join , c:Studio__Studio_FK]) )); "
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
    												
    												
    												
    												
    												+"(12 + (4 - 8)) * (15 / 5) + 5;"
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
   tree.apply(new Translation2());
  }
  catch(Exception e)
  {
   System.out.println(e.getMessage());
   e.printStackTrace();
  }
 }
}