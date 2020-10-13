package no.ssb.lds.core.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tika.mime.MediaType;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;

public class BodyParser {

    static final ObjectMapper jsonMapper = new ObjectMapper();
    static final ObjectMapper msgPackMapper = new ObjectMapper(new MessagePackFactory());

    public static JsonNode deserializeBody(String contentType, String requestBody) {
        try {
            // deserialize request data
            MediaType type = MediaType.parse(contentType);
            JsonNode requestData;
            if ("application".equals(type.getType()) && "json".equals(type.getSubtype())) {
                requestData = jsonMapper.readTree(requestBody);
            } else if ("application".equals(type.getType())
                    && ("msgpack".equals(type.getSubtype()) || "x-msgpack".equals(type.getSubtype()))) {
                requestData = msgPackMapper.readTree(requestBody);
            } else {
                throw new IllegalArgumentException("Unsupported Content-Type: " + contentType);
            }
            return requestData;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
