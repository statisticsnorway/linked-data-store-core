package no.ssb.lds.graphql.jsonSchema;

import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.schema.*;
import no.ssb.lds.graphql.jsonSchema.visitors.ParseObjectTypesVisitor;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GraphQLToJsonConverter {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLToJsonConverter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeTraverser TRAVERSER = new TypeTraverser();
    GraphQLSchema graphQLSchema;

    public GraphQLToJsonConverter(GraphQLSchema schema) {
        this.graphQLSchema = schema;
    }

    public LinkedHashMap<String, JSONObject>  createSpecification(GraphQLSchema graphQLSchema) {
        Map<String, GraphQLType> typeMap = graphQLSchema.getTypeMap();
        LinkedHashMap<String, JSONObject> jsonMap = new LinkedHashMap<>();
        StringBuilder sb = new StringBuilder();

        ParseObjectTypesVisitor parseObjectTypesVisitor = new ParseObjectTypesVisitor(typeMap, jsonMap);
        TRAVERSER.depthFirst(parseObjectTypesVisitor, typeMap.values());

        jsonMap.forEach((managedDomain, jsonObject) -> {
            sb.append(" /" + managedDomain);
        });

        LOG.info("{}", (sb.length() == 0 ? "No schemas configured!" : "Managed domains: " + sb.toString().substring(1)));

        return jsonMap;
    }
}
