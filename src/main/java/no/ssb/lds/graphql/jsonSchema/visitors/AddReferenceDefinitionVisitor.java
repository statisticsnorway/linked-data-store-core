package no.ssb.lds.graphql.jsonSchema.visitors;

import graphql.schema.*;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class AddReferenceDefinitionVisitor extends GraphQLTypeVisitorStub {
    private static final Logger LOG = LoggerFactory.getLogger(AddReferenceDefinitionVisitor.class);
    private GraphQLObjectType node;
    private JSONObject jsonElements;
    private String visitedFieldName;
    private ArrayList<String> requiredProperties = new ArrayList<>();
    private static String lastVisitedNodeType;

    public AddReferenceDefinitionVisitor(GraphQLObjectType node, JSONObject jsonElements) {
        this.node = node;
        this.jsonElements = jsonElements;
    }

    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        return TraversalControl.CONTINUE;
    }

    @Override
    public TraversalControl visitGraphQLFieldDefinition(GraphQLFieldDefinition node, TraverserContext<GraphQLType> context) {
        //LOG.info("Parsing field:: [{}]-->[{}]", this.node.getName(), node.getName());

        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(this.node.getName());
        JSONObject definitionProperties = (JSONObject) definitionElements.get("properties");
        JSONObject propertyElements = new JSONObject();

        GraphQLOutputType type = node.getType();

        if (GraphQLTypeUtil.isNonNull(type)) {
            JSONArray defRequiredProperties = (JSONArray) definitionElements.get("required");
            defRequiredProperties.put(node.getName());
        }

        GraphQLType graphQLType = GraphQLTypeUtil.unwrapOne(node.getType());
        if (GraphQLTypeUtil.isScalar(graphQLType)) {
            if (graphQLType.getName().equalsIgnoreCase("string")) {
                propertyElements.put("type", "string");
            }

            switch (graphQLType.getName()) {
                case "String":
                    propertyElements.put("type", "string");
                    break;
                case "DateTime":
                    propertyElements.put("type", "string");
                    propertyElements.put("format", "date-time");
                    break;
            }
        }

        propertyElements.put("description", node.getDescription());
        propertyElements.put("displayName", "");

        definitionProperties.put(node.getName(), propertyElements);

        visitedFieldName = node.getName();
        lastVisitedNodeType = GraphQLFieldDefinition.class.getName();

        return visitGraphQLType(node, context);
    }

    @Override
    public TraversalControl visitGraphQLEnumType(GraphQLEnumType node, TraverserContext<GraphQLType> context) {
        //LOG.info("Parsing enum:: [{}]-->[{}]", this.node.getName(), node.getName());

        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(this.node.getName());
        JSONObject definitionProperties = (JSONObject) definitionElements.get("properties");
        JSONObject fieldProperties = (JSONObject) definitionProperties.get(visitedFieldName);

        ArrayList<String> enumValues = new ArrayList<>();

        node.getValues().stream().forEach(value -> {
            enumValues.add(value.getName());
        });

        fieldProperties.put("type", "string");

        fieldProperties.put("enum", enumValues);

        lastVisitedNodeType = GraphQLEnumType.class.getName();

        return visitGraphQLType(node, context);
    }

    @Override
    public TraversalControl visitGraphQLList(GraphQLList node, TraverserContext<GraphQLType> context) {
        //LOG.info("Parsing list:: [{}]-->[{}]", this.node.getName(), visitedFieldName);

        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(this.node.getName());
        JSONObject definitionProperties = (JSONObject) definitionElements.get("properties");

        JSONObject fieldProperties = (JSONObject) definitionProperties.get(visitedFieldName);
        fieldProperties.put("type", "array");

        JSONObject itemList = new JSONObject();

        GraphQLType type = ((GraphQLNonNull) node.getWrappedType()).getWrappedType();

        if (type instanceof GraphQLObjectType) {
            itemList.put("$ref", "#/definitions/" + type.getName());
        } else if (type instanceof GraphQLScalarType) {
            if (type.getName().equalsIgnoreCase("string")) {
                itemList.put("type", "string");
            }
        }

        fieldProperties.put("items", itemList);

        lastVisitedNodeType = GraphQLList.class.getName();

        return visitGraphQLType(node, context);
    }

    @Override
    public TraversalControl visitGraphQLNonNull(GraphQLNonNull node, TraverserContext<GraphQLType> context) {
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(this.node.getName());
        JSONArray defRequiredProperties = (JSONArray) definitionElements.get("required");

        if (!lastVisitedNodeType.equalsIgnoreCase("graphql.schema.GraphQLList")) {
            if (!defRequiredProperties.toList().contains(visitedFieldName)) {
                defRequiredProperties.put(visitedFieldName);
            }
        }

        return visitGraphQLType(node, context);
    }
}
