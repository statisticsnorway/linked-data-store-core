
package no.ssb.lds.graphql.jsonSchema;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLEnumValueDefinition;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNamedSchemaElement;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchemaElement;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeUtil;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import no.ssb.lds.graphql.directives.LinkDirective;
import no.ssb.lds.graphqlneo4j.GraphQLNeo4jTBVLanguage;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class JsonSchemaGenerator extends GraphQLTypeVisitorStub {

    static class ObjectContext {
        final String name;
        final JSONObject object;

        ObjectContext(String name, JSONObject object) {
            this.name = name;
            this.object = object;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaGenerator.class);

    private final TypeDefinitionRegistry typeDefinitionRegistry;
    private final Map<String, JSONObject> jsonMap;
    private final Set<String> domains = new LinkedHashSet<>();
    private final Map<String, JSONObject> types = new LinkedHashMap<>();
    private final Map<String, Set<String>> references = new LinkedHashMap<>();

    public JsonSchemaGenerator(TypeDefinitionRegistry typeDefinitionRegistry, Map<String, JSONObject> jsonMap) {
        this.typeDefinitionRegistry = typeDefinitionRegistry;
        this.jsonMap = jsonMap;
    }

    public void resolveDomainReferences() {
        for (String domain : domains) {
            JSONObject definitions = (JSONObject) jsonMap.get(domain).get("definitions");
            resolveReferencesAndPopulateSchemaDefinitions(definitions, domain);
        }
    }

    private void resolveReferencesAndPopulateSchemaDefinitions(JSONObject schemaDefinitions, String referenceSubject) {
        Set<String> directReferences = references.computeIfAbsent(referenceSubject, rs -> new LinkedHashSet<>());
        for (String directReference : directReferences) {
            if (!schemaDefinitions.has(directReference)) {
                schemaDefinitions.put(directReference, types.get(directReference));
                resolveReferencesAndPopulateSchemaDefinitions(schemaDefinitions, directReference);
            }
        }
    }

    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLSchemaElement> context) {

        if (node.getName().startsWith("_")) {
            // builtin types
            return TraversalControl.ABORT;
        }

        if ("Query".equalsIgnoreCase(node.getName())) {
            // query type
            return TraversalControl.ABORT;
        }

        JSONObject definition = new JSONObject();
        types.put(node.getName(), definition);

        context.setVar(ObjectContext.class, new ObjectContext(node.getName(), definition));

        definition.put("type", "object");
        definition.put("properties", new JSONObject());
        definition.put("required", new JSONArray());

        String description = node.getDescription();
        // Using the first line of the description as display name if possible.
        if (description != null) {
            String displayName = Stream.of(description.split(System.lineSeparator()))
                    .findFirst().map(String::strip).orElse(description);
            definition.put("displayName", displayName);

            definition.put("description", description.strip());
        }

        List<GraphQLDirective> directives = node.getDirectives();
        boolean isDomainType = directives.stream().map(GraphQLDirective::getName).anyMatch("domain"::equalsIgnoreCase);

        if (isDomainType) {
            JSONObject jsonDefinitions = new JSONObject();
            jsonDefinitions.put(node.getName(), definition);

            JSONObject jsonElements = new JSONObject();
            jsonElements.put("$ref", "#/definitions/" + node.getName());
            jsonElements.put("definitions", jsonDefinitions);
            jsonElements.put("#schema", "http://json-schema.org/draft-04/schema#");

            jsonMap.put(node.getName(), jsonElements);
            domains.add(node.getName());

            return TraversalControl.CONTINUE;
        }

        types.put(node.getName(), definition);

        return TraversalControl.CONTINUE;
    }

    @Override
    public TraversalControl visitGraphQLInterfaceType(GraphQLInterfaceType node, TraverserContext<GraphQLSchemaElement> context) {
        return TraversalControl.ABORT; // interfaces not needed, fields are repeated in implementing objects.
    }

    @Override
    public TraversalControl visitGraphQLFieldDefinition(GraphQLFieldDefinition node, TraverserContext<GraphQLSchemaElement> context) {

        ObjectContext objectContext = context.getVarFromParents(ObjectContext.class);
        if (objectContext == null) {
            throw new IllegalStateException("Missing JsonDefinitionContext");
        }

        JSONObject definitionProperties = (JSONObject) objectContext.object.get("properties");

        JSONObject propertyElements = new JSONObject();
        definitionProperties.put(node.getName(), propertyElements);

        // add non-null type to the required list
        GraphQLType type = node.getType();
        if (GraphQLTypeUtil.isNonNull(type)) {
            JSONArray defRequiredProperties = (JSONArray) objectContext.object.get("required");
            defRequiredProperties.put(node.getName());
            type = GraphQLTypeUtil.unwrapNonNull(type);
        }

        // put non-blank description in json-schema
        if (node.getDescription() != null && !node.getDescription().isBlank()) {
            propertyElements.put("description", node.getDescription());
        }

        // wrap json-schema type in items
        if (GraphQLTypeUtil.isList(type)) {
            JSONObject listType = new JSONObject();
            propertyElements.put("type", "array");
            propertyElements.put("items", listType);
            propertyElements = listType;
            type = GraphQLTypeUtil.unwrapOne(type);
        }

        // ignore double wrapped non-null
        if (GraphQLTypeUtil.isNonNull(type)) {
            type = GraphQLTypeUtil.unwrapNonNull(type);
        }

        // add link node
        if (node.getDirective(LinkDirective.NAME) != null) {
            propertyElements.put("type", "string");

            // link metadata field
            JSONObject fieldObjects = new JSONObject();
            JSONObject properties = new JSONObject();
            fieldObjects.put("properties", properties);
            fieldObjects.put("type", "object");
            definitionProperties.put("_link_property_" + node.getName(), fieldObjects);
            List<String> concreteTypes = GraphQLNeo4jTBVLanguage.resolveAbstractTypeToConcreteTypes(typeDefinitionRegistry, ((GraphQLNamedSchemaElement) type).getName());
            for (String concreteType : concreteTypes) {
                properties.put(concreteType, new JSONObject()
                        .put("type", "null"));
            }

            return TraversalControl.ABORT;
        }

        // scalar
        if (type instanceof GraphQLScalarType) {
            switch (((GraphQLNamedSchemaElement) type).getName()) {
                case "Int":
                    propertyElements.put("type", "integer");
                    break;
                case "Float":
                    propertyElements.put("type", "number");
                    break;
                case "String":
                case "ID":
                    propertyElements.put("type", "string");
                    break;
                case "Boolean":
                    propertyElements.put("type", "boolean");
                    break;
                case "DateTime":
                    propertyElements.put("type", "string");
                    propertyElements.put("format", "date-time");
                    break;
                case "Date":
                    propertyElements.put("type", "string");
                    propertyElements.put("format", "date");
                    break;
                case "Time":
                    propertyElements.put("type", "string");
                    propertyElements.put("format", "time");
                    break;
                default:
                    LOG.error("MISSING SCALAR CASE: {}", ((GraphQLNamedSchemaElement) type).getName());
            }

            return TraversalControl.ABORT;
        }

        // embedded object fields
        if (type instanceof GraphQLObjectType) {
            if (Set.of("_Neo4jDateTime", "_Neo4jLocalDateTime").contains(((GraphQLNamedSchemaElement) type).getName())) {
                propertyElements.put("type", "string");
                propertyElements.put("format", "date-time");
                return TraversalControl.ABORT;
            }
            if (Set.of("_Neo4jTime", "_Neo4jLocalTime").contains(((GraphQLNamedSchemaElement) type).getName())) {
                propertyElements.put("type", "string");
                propertyElements.put("format", "time");
                return TraversalControl.ABORT;
            }
            if (Set.of("_Neo4jDate").contains(((GraphQLNamedSchemaElement) type).getName())) {
                propertyElements.put("type", "string");
                propertyElements.put("format", "date");
                return TraversalControl.ABORT;
            }

            propertyElements.put("$ref", "#/definitions/" + ((GraphQLNamedSchemaElement) type).getName());
            references.computeIfAbsent(objectContext.name, rs -> new LinkedHashSet<>())
                    .add(((GraphQLNamedSchemaElement) type).getName());
            return TraversalControl.ABORT;
        }

        // enum fields
        if (type instanceof GraphQLEnumType) {
            propertyElements.put("$ref", "#/definitions/" + ((GraphQLNamedSchemaElement) type).getName());
            references.computeIfAbsent(objectContext.name, rs -> new LinkedHashSet<>())
                    .add(((GraphQLNamedSchemaElement) type).getName());

            return TraversalControl.ABORT;
        }

        throw new IllegalArgumentException("Unsupported field type: " + ((GraphQLNamedSchemaElement) type).getName());
    }

    @Override
    public TraversalControl visitGraphQLDirective(GraphQLDirective node, TraverserContext<GraphQLSchemaElement> context) {
        return TraversalControl.ABORT;
    }

    @Override
    public TraversalControl visitGraphQLEnumType(GraphQLEnumType node, TraverserContext<GraphQLSchemaElement> context) {
        JSONObject type = new JSONObject();
        JSONArray enumValues = new JSONArray();
        for (GraphQLEnumValueDefinition value : node.getValues()) {
            enumValues.put(value.getName());
        }
        type.put("type", "string");
        type.put("enum", enumValues);
        types.put(node.getName(), type);
        return TraversalControl.ABORT;
    }

    @Override
    public TraversalControl visitGraphQLInputObjectType(GraphQLInputObjectType node, TraverserContext<GraphQLSchemaElement> context) {
        return TraversalControl.ABORT;
    }

    @Override
    public TraversalControl visitGraphQLArgument(GraphQLArgument node, TraverserContext<GraphQLSchemaElement> context) {
        return TraversalControl.ABORT;
    }

    @Override
    public TraversalControl visitGraphQLList(GraphQLList node, TraverserContext<GraphQLSchemaElement> context) {
        return TraversalControl.ABORT;
    }
}

