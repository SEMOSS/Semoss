/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.rdf.main;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.util.Vector;

import javax.xml.bind.DatatypeConverter;

import prerna.rdf.engine.impl.SesameJenaConstructStatement;

import com.google.gson.GsonBuilder;

/**
 */
public class GsonTester {
	/**
	 * Method main.
	 * @param args String[]
	 */
	public static void main(String [] args)
	{
		Hashtable hash = new Hashtable();
		hash.put("1", "arg1");
		hash.put("2", "arg1");
		hash.put("3", "arg1");
		hash.put("4", "arg1");
		Hashtable hash2 = new Hashtable();
		hash2.put("1", "arg1");
		hash2.put("2", "arg1");
		hash2.put("3", "arg1");
		hash2.put("4", "arg1");
		hash2.put("num", new Integer(2));
		hash2.put("bool", true);
		hash.put("Hash", hash2);
		
		Vector vec = new Vector();
		vec.add(1);
		vec.add(2);
		
		SesameJenaConstructStatement stmt = new SesameJenaConstructStatement();
		stmt.setSubject("Hola");
		stmt.setPredicate("is");
		stmt.setObject("Hello");

		boolean data = true;
		
		GsonBuilder gson = new GsonBuilder();
		/*gson.registerTypeAdapter(Integer.class,  new JsonSerializer<Integer>() {   
		    @Override
		    public JsonElement serialize(Integer src, Type typeOfSrc, JsonSerializationContext context) {
		    	System.out.println("Type of serialize... " + typeOfSrc);
		        if(src == src.longValue())
		            return new JsonPrimitive(src.intValue());          
		        return new JsonPrimitive(src);
		    }
		 });
		gson.registerTypeAdapter(Integer.class, new JsonDeserializer<Integer>() {

			@Override
			public Integer deserialize(JsonElement json, Type arg1,
					JsonDeserializationContext arg2) throws JsonParseException {
				// TODO Auto-generated method stub
				System.out.println("Json element is " + json);
				return new Integer(json.getAsJsonPrimitive().getAsString());
			}
			 
		});*/
		 //gson.create();
		
		String rep = gson.create().toJson(hash2);
		Hashtable data2 = gson.create().fromJson(rep, Hashtable.class);
		
		
		System.out.println("Representation is " + rep);
		
		double dataBack = Double.parseDouble(data2.get("num")+"");

		Double myDouble = (Double)data2.get("num");
		
		//Integer intData = (Integer)data2.get("num");
		
		//System.out.println("Integer output is " + intData);
		System.out.println(" Type is " + data2.get("num").getClass());
		System.out.println(" Type is " + data2.get("bool").getClass());
		
		System.out.println("JSON looks like this" + rep + myDouble);
		
		//SesameJenaConstructStatement stmt2 = gson.fromJson(rep, SesameJenaConstructStatement.class);
		
		//Hashtable newVec = gson.fromJson(rep, Hashtable.class);
		
		//System.out.println("Vector is " + stmt2.getSubject());//("Hash"));
		
		System.out.println(gson.create().toJson(hash));
		
		System.out.println("Trying object routines");
		try {
			String byteRep;
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(hash2);
			oos.flush();
			oos.close();
			
			byteRep = DatatypeConverter.printBase64Binary(bos.toByteArray());
			//byteRep = bos.toString();
			
			System.out.println("Object has been written");
			System.out.println("Reading now");
			
//			byte [] repData = BASE64Decoder.decodeBuffer( byteRep );
			Hashtable retTab = (Hashtable) new ObjectInputStream(new ByteArrayInputStream(DatatypeConverter.parseBase64Binary(byteRep))).readObject();
			System.out.println("Ret Tab is " + retTab);
			
			
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
