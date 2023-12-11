package prerna.util.gson;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.Date;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import cern.colt.Arrays;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;

public class IHeadersDataRowAdapter extends AbstractSemossTypeAdapter<IHeadersDataRow> {
	
	private enum ValueType {

		// String
		STRING,
		
		// Null
		NULL,
		
		// The primitive types
		INT,
		DOUBLE,
		LONG,
		FLOAT,
		BOOLEAN,
		CHAR,
		BYTE,
		SHORT,
		
		// Anything else
		ENCODED;
	}
	
	@Override
	public IHeadersDataRow read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		// should start with the type
		in.beginObject();
		if (in.peek() != JsonToken.NAME || !in.nextName().equals("type")) {
			throw new IllegalArgumentException("Header is not serialized properly. Must start with type.");
		}

		String headerTypeString = in.nextString();
		IHeadersDataRow.HEADERS_DATA_ROW_TYPE headerType = IHeadersDataRow.convertStringToHeaderType(headerTypeString);
		
		TypeAdapter<IHeadersDataRow> reader = IHeadersDataRow.getAdapterForHeader(headerType);
		return reader.read(in);
	}
	
	@Override
	public void write(JsonWriter out, IHeadersDataRow value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		// go to the specific instance impl to write it
		IHeadersDataRow.HEADERS_DATA_ROW_TYPE headerType = value.getHeaderType();
		TypeAdapter<IHeadersDataRow> reader = IHeadersDataRow.getAdapterForHeader(headerType);
				
		reader.write(out, value);
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////
	///////////////// Methods for serializing and deserializing row values ///////////////////
	
	public static SerializedValuesAndTypes serializeValues(Object[] values) {
		return new SerializedValuesAndTypes(values);
	}
	
	public static Object[] deserializeValues(String[] serializedValues, String[] serializedValueTypes) {
		Object[] deserializedValues = new Object[serializedValues.length];
		for (int i = 0; i < serializedValues.length; i++) {
			ValueType valueType = ValueType.valueOf(serializedValueTypes[i]);
			String serializedValue = serializedValues[i];
			switch (valueType) {
				case STRING:
					deserializedValues[i] = serializedValue;
					break;
				case NULL:
					deserializedValues[i] = null;
					break;
				case INT:
					deserializedValues[i] = Integer.parseInt(serializedValue);
					break;
				case DOUBLE:
					deserializedValues[i] = Double.parseDouble(serializedValue);
					break;
				case LONG:
					deserializedValues[i] = Long.parseLong(serializedValue);
					break;
				case FLOAT:
					deserializedValues[i] = Float.parseFloat(serializedValue);
					break;
				case BOOLEAN:
					deserializedValues[i] = Boolean.parseBoolean(serializedValue);
					break;
				case CHAR:
					deserializedValues[i] = serializedValue.charAt(0);
					break;
				case BYTE:
					deserializedValues[i] = Byte.parseByte(serializedValue);
					break;
				case SHORT:
					deserializedValues[i] = Short.parseShort(serializedValue);
					break;
				case ENCODED:
					deserializedValues[i] = fromEncodedString(serializedValue);
					break;
				default:
					deserializedValues[i] = serializedValue;
					break;
			}
		}
		return deserializedValues;
	}
		
	public static class SerializedValuesAndTypes {
		
		private String[] serializedValues;
		private String[] serializedValueTypes;
		
		public SerializedValuesAndTypes(Object[] values) {
			serializedValueTypes = new String[values.length];
			serializedValues = new String[values.length];
			for (int i = 0; i < values.length; i++) {
				Object value = values[i];
				if (value == null) {
					serializedValueTypes[i] = ValueType.NULL.name();
					serializedValues[i] = null;
				} else if (value instanceof String) {
					serializedValueTypes[i] = ValueType.STRING.name();
					serializedValues[i] = value.toString();
				} else if (value instanceof Integer) {
					serializedValueTypes[i] = ValueType.INT.name();
					serializedValues[i] = value.toString();
				} else if (value instanceof Double) {
					serializedValueTypes[i] = ValueType.DOUBLE.name();
					serializedValues[i] = value.toString();
				} else if (value instanceof Long) {
					serializedValueTypes[i] = ValueType.LONG.name();
					serializedValues[i] = value.toString();
				} else if (value instanceof Float) {
					serializedValueTypes[i] = ValueType.FLOAT.name();
					serializedValues[i] = value.toString();
				} else if (value instanceof Boolean) {
					serializedValueTypes[i] = ValueType.BOOLEAN.name();
					serializedValues[i] = value.toString();
				} else if (value instanceof Character) {
					serializedValueTypes[i] = ValueType.CHAR.name();
					serializedValues[i] = value.toString();
				} else if (value instanceof Byte) {
					serializedValueTypes[i] = ValueType.BYTE.name();
					serializedValues[i] = value.toString();
				} else if (value instanceof Short) {
					serializedValueTypes[i] = ValueType.SHORT.name();
					serializedValues[i] = value.toString();
				} else {
					serializedValueTypes[i] = ValueType.ENCODED.name();
					serializedValues[i] = toEncodedString(value);
				}
			}
		}
		
		public String[] getSerializedValues() {
			return serializedValues;
		}
		
		public String[] getSerializedValueTypes() {
			return serializedValueTypes;
		}
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////// Helper methods //////////////////////////////////////////
	
	private static Object fromEncodedString(String s) {
		if (s == null) {
			return null;
		} else try {
			byte[] data = Base64.getDecoder().decode(s);
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
			Object o = ois.readObject();
			ois.close();
			return o;
		} catch (IOException | ClassNotFoundException e) {
			return null;
		}
	}

	private static String toEncodedString(Object o) {
		
		// If null or not serializable, then return null
		if (o == null || !(o instanceof Serializable)) {
			return null;
			
		// Else try encoding in base 64
		} else try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(o);
			oos.close();
			return Base64.getEncoder().encodeToString(baos.toByteArray());
		} catch (IOException e) {
			return null;
		}
	}

	public static String toPrettyFormat(String jsonString) {
		return new GsonBuilder().setPrettyPrinting().create()
				.toJson(new JsonParser().parse(jsonString).getAsJsonObject());
	}
	
//	public static void main(String[] args) throws IOException {
//		String[] headers = new String[] {"STRING_H", "NULL_H", "INT_H", "DOUBLE_H", "LONG_H", "FLOAT_H", "BOOLEAN_H", "CHAR_H", "BYTE_H", "SHORT_H", "ENCODED_H"};
//		String[] rawHeaders = new String[] {"R_STRING_H", "R_NULL_H", "R_INT_H", "R_DOUBLE_H", "R_LONG_H", "R_FLOAT_H", "R_BOOLEAN_H", "R_CHAR_H", "R_BYTE_H", "R_SHORT_H", "R_ENCODED_H"};
//		Object[] values = new Object[] {"foo", null, 1, 2.1D, 3L, 4.1F, true, 'a', new Byte("0"), new Short("1"), new Date()};
//		Object[] rawValues = new Object[] {"R_foo", null, 2, 3000000000.0D, 4L, 5.1F, false, 'b', new Byte("1"), new Short("0"), new Date()};
//		System.out.println(">>>");
//		for (int i = 0; i < 3; i++) {
//			SerializedValuesAndTypes result = serializeValues(values);
//			System.out.println(Arrays.toString(values));
//			System.out.println(Arrays.toString(result.getSerializedValues()));
//			System.out.println(Arrays.toString(result.getSerializedValueTypes()));
//			values = deserializeValues(result.getSerializedValues(), result.getSerializedValueTypes());
//			System.out.println(">>>");
//		}
//		IHeadersDataRow dataRow = new HeadersDataRow(headers, rawHeaders, values, rawValues);
//		System.out.println(dataRow.toRawString());
//		System.out.println(">>>");
//		IHeadersDataRowAdapter adapter = new IHeadersDataRowAdapter();
//		System.out.println(toPrettyFormat(adapter.toJson(dataRow)));
//		System.out.println(">>>");
//		IHeadersDataRow dataRow2 = adapter.fromJson(adapter.toJson(dataRow));
//		System.out.println(dataRow2.toRawString());
//		System.out.println(">>>");
//		System.out.println(dataRow.toRawString().equals(dataRow2.toRawString()));
//		System.out.println(">>>");
//	}
	
}
