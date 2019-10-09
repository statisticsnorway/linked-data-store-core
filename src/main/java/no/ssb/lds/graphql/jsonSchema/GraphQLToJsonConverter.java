package no.ssb.lds.graphql.jsonSchema;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.language.Directive;
import graphql.language.ObjectTypeDefinition;
import graphql.language.SchemaDefinition;
import graphql.schema.*;
import no.ssb.lds.api.specification.SpecificationElement;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static graphql.introspection.IntrospectionQuery.INTROSPECTION_QUERY;

public class GraphQLToJsonConverter {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLToJsonConverter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    GraphQLSchema graphQLSchema;
    GraphQL GRAPHQL;

    public GraphQLToJsonConverter(GraphQLSchema schema) {
        this.graphQLSchema = schema;
    }

    public void parseGraphQLSchema(GraphQLSchema schema) {

        GRAPHQL = new GraphQL(schema);

        LOG.info("GraphQLSchema to be parsed: ");
        Map<String, String> schemaByName = null;

        Map<String, GraphQLType> typeMap = schema.getTypeMap();

        Set<GraphQLType> additionalTypes = schema.getAdditionalTypes();

        typeMap.entrySet().stream().forEach(e ->
                {
                    GraphQLType type = e.getValue();
                    JSONObject jsonObject = new JSONObject();
                    if (type instanceof GraphQLObjectType) {
                        System.out.println("OBJECT TYPE");
                        System.out.println(type.getName());
                        ObjectTypeDefinition definition = ((GraphQLObjectType) type).getDefinition();
                        Optional<Directive> domainType = null;
                        if(definition != null){
                            domainType  = ((GraphQLObjectType) type).getDefinition().getDirectives().stream()
                                    .filter(directive -> directive.getName().equalsIgnoreCase("domain")).findFirst();

                            if(domainType.isPresent()){
                                System.out.println(domainType.get().getName());
                                System.out.println("Key : " + e.getKey());
                                System.out.println("Type : " + e.getValue());

                                jsonObject.append("$ref", "#/definitions/"+e.getKey());
                                String jsonStr = jsonObject.toString(4);
                                System.out.println(jsonStr);

                                ObjectMapper MAPPER = new ObjectMapper();
                                MAPPER.enable(SerializationFeature.INDENT_OUTPUT);

                                ExecutionResult executionResult = executeQuery();
                                LinkedHashMap data = executionResult.getData();

                                LinkedHashMap schemaValue = (LinkedHashMap) data.get("__schema");
                                ArrayList typeList = (ArrayList) schemaValue.get("types");

                                JSONArray jsonArray = new JSONArray(typeList);
                                System.out.println();
                                List<GraphQLFieldDefinition> fieldDefinitions = ((GraphQLObjectType) type).getFieldDefinitions();
                                fieldDefinitions.forEach(def -> {
                                    System.out.println(def);
                                });
                            }
                        }
                    }
                });
    }


    private byte[] jsonSchema() throws JsonProcessingException {
        return MAPPER.writeValueAsString(executeQuery()).getBytes();
    }

    private ExecutionResult executeQuery() {
        return GRAPHQL.execute(INTROSPECTION_QUERY);
    }
}
