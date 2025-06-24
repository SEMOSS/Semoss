package prerna.playground.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import org.apache.commons.text.StringEscapeUtils;

public class CustomMapper extends SimpleModule {

  /** */
  private static final long serialVersionUID = 7508060578903475428L;

  // used to translate incoming noun store to a desired object type
  // incoming arguments are loaded into map with form {String: Vector<Object>, String:
  // Vector<Object>}
  // mapping to a class serializes that map, then deserializes to the object
  public static final ObjectMapper PAYLOAD_MAPPER =
      JsonMapper.builder()
          .enable(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED)
          .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
          .enable(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS)
          .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
          .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
          .build()
          .registerModule(new CustomMapper());

  // used to translate an object type to a result map
  // object is serialized and deserialized to map
  public static final ObjectMapper MAPPER =
      JsonMapper.builder()
          .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
          .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
          .build()
          .registerModule(new CustomMapper());

  public CustomMapper() {

    addDeserializer(
        String.class,
        new StdDeserializer<String>(String.class) {
          private static final long serialVersionUID = -4179939507094287588L;

          @Override
          public String deserialize(final JsonParser jp, final DeserializationContext ctxt)
              throws IOException, JsonProcessingException {
            String val = jp.getText();
            if (val == null) {
              return null;
            }
            return StringEscapeUtils.unescapeJson(val);
          }
        });
  }
}
