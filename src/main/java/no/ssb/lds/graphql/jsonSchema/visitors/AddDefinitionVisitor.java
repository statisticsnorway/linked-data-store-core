package no.ssb.lds.graphql.jsonSchema.visitors;

import graphql.language.FieldDefinition;
import graphql.schema.*;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import no.ssb.lds.graphql.directives.LinkDirective;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static no.ssb.lds.graphql.directives.DomainDirective.hasDomainDirective;


public class AddDefinitionVisitor extends GraphQLTypeVisitorStub {
    private static final Logger LOG = LoggerFactory.getLogger(AddDefinitionVisitor.class);
    private static final TypeTraverser TRAVERSER = new TypeTraverser();

    private GraphQLObjectType node;
    private JSONObject jsonElements;
    private String visitedFieldName;
    private ArrayList<String> requiredProperties;
    private static String lastVisitedNodeType;

    public AddDefinitionVisitor(GraphQLObjectType node, JSONObject jsonElements) {
        this.node = node;
        this.jsonElements = jsonElements;
    }

    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        if ((node.getName().indexOf("Connection") != -1)) {
            return visitGraphQLConnection(node, context);
        } else {
            List<GraphQLDirective> directives = node.getDirectives();
            boolean isDomainType = directives.stream().map(GraphQLDirective::getName).anyMatch("domain"::equalsIgnoreCase);

            if (isDomainType) {
                return visitGraphQLDomainType(node, context);
            } else {
                JSONObject definitionElements = new JSONObject();
                definitionElements.put("type", "object");
                definitionElements.put("properties", new JSONObject());
                definitionElements.put("required", new ArrayList<>());

                JSONObject definitions = (JSONObject) jsonElements.get("definitions");
                definitions.put(node.getName(), definitionElements);

                AddReferenceDefinitionVisitor addReferenceDefinitionVisitor = new AddReferenceDefinitionVisitor(node, jsonElements);
                List<GraphQLFieldDefinition> graphQLFieldDefinitions = node.getFieldDefinitions();
                List<FieldDefinition> fieldDefinitions = node.getDefinition().getFieldDefinitions();

                TRAVERSER.depthFirst(addReferenceDefinitionVisitor, graphQLFieldDefinitions);

                return TraversalControl.ABORT;
            }
        }
    }

    @Override
    public TraversalControl visitGraphQLFieldDefinition(GraphQLFieldDefinition node, TraverserContext<GraphQLType> context) {
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(this.node.getName());
        JSONObject definitionProperties = (JSONObject) definitionElements.get("properties");
        JSONObject propertyElements = new JSONObject();

        GraphQLOutputType type = node.getType();
        visitedFieldName = node.getName();

        lastVisitedNodeType = GraphQLFieldDefinition.class.getName();
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


        for (GraphQLDirective directive : node.getDirectives()) {
            if (directive.getName().equals(LinkDirective.NAME)) {
                if (GraphQLTypeUtil.isList(node.getType())) {
                    GraphQLType objectType = ((GraphQLList) node.getType()).getWrappedType();
                    if (objectType instanceof GraphQLUnionType) {
                        visitUnionLinkProperty(objectType, context);
                        return TraversalControl.ABORT;
                    } else {
                        visitListLinkProperty(objectType, context);
                        return TraversalControl.ABORT;
                    }
                }else{
                    return visitGraphQLType(node.getType(), context);
                }
            }
        }

        return visitGraphQLType(node.getType(), context);
    }

    @Override
    public TraversalControl visitGraphQLEnumType(GraphQLEnumType node, TraverserContext<GraphQLType> context) {
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
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(this.node.getName());
        JSONObject definitionProperties = (JSONObject) definitionElements.get("properties");

        JSONObject fieldProperties = (JSONObject) definitionProperties.get(visitedFieldName);
        fieldProperties.put("type", "array");

        JSONObject itemList = new JSONObject();

        GraphQLType type;

        if (GraphQLTypeUtil.isNonNull(node.getWrappedType())) {
            type = ((GraphQLNonNull) node.getWrappedType()).getWrappedType();
        } else {
            type = node.getWrappedType();
        }

        if (type instanceof GraphQLObjectType) {
            if(hasDomainDirective((GraphQLObjectType)type)){
                if (type instanceof GraphQLUnionType) {
                    visitUnionLinkProperty(type, context);
                    return TraversalControl.ABORT;
                } else {
                    visitListLinkProperty(type, context);
                    return TraversalControl.ABORT;
                }
            }else{
                itemList.put("$ref", "#/definitions/" + type.getName());
            }
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
    public TraversalControl visitGraphQLScalarType(GraphQLScalarType node, TraverserContext<GraphQLType> context) {
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

        GraphQLType graphQLType = GraphQLTypeUtil.unwrapOne(node);

        if (GraphQLTypeUtil.isList(GraphQLTypeUtil.unwrapOne(node))) {
            visitGraphQLList((GraphQLList) graphQLType, context);
        }
        return visitGraphQLType(node.getWrappedType(), context);
    }

    public TraversalControl visitGraphQLConnection(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(this.node.getName());
        JSONObject definitionProperties = (JSONObject) definitionElements.get("properties");

        JSONObject fieldProperties = (JSONObject) definitionProperties.get(visitedFieldName);
        fieldProperties.put("type", "array");

        JSONObject itemList = new JSONObject();
        itemList.put("type", "string");

        fieldProperties.put("items", itemList);

        return TraversalControl.ABORT;
    }

    public TraversalControl visitGraphQLDomainType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(this.node.getName());
        JSONObject definitionProperties = (JSONObject) definitionElements.get("properties");

        JSONObject fieldProperties = (JSONObject) definitionProperties.get(visitedFieldName);
        fieldProperties.put("type", "string");
        fieldProperties.put("displayName", "");
        fieldProperties.put("description", node.getDescription());

        JSONObject properties = new JSONObject();
        JSONObject propertyElements = new JSONObject();
        JSONObject fieldObjects = new JSONObject();

        propertyElements.put("type", "null");
        properties.put(node.getName(), propertyElements);
        fieldObjects.put("properties", properties);
        fieldObjects.put("type", "object");

        definitionProperties.put("_link_property_" + visitedFieldName, fieldObjects);

        return TraversalControl.ABORT;
    }

    public TraversalControl visitListLinkProperty(GraphQLType node, TraverserContext<GraphQLType> context) {
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(this.node.getName());
        JSONObject definitionsProperties = (JSONObject) definitionElements.get("properties");
        JSONObject definitionProperties = (JSONObject) definitionsProperties.get(visitedFieldName);

        JSONObject fieldObjects = new JSONObject();
        JSONObject properties = new JSONObject();
        JSONObject propertyElements = new JSONObject();
        JSONObject itemList = new JSONObject();

        definitionProperties.put("type", "array");
        itemList.put("type", "string");
        definitionProperties.put("items", itemList);

        definitionsProperties.put(visitedFieldName, definitionProperties);

        properties = new JSONObject();
        propertyElements.put("type", "null");
        properties.put(node.getName(), propertyElements);
        fieldObjects.put("properties", properties);
        fieldObjects.put("type", "object");

        definitionsProperties.put("_link_property_" + visitedFieldName, fieldObjects);


        return TraversalControl.ABORT;
    }

    public TraversalControl visitUnionLinkProperty(GraphQLType node, TraverserContext<GraphQLType> context) {
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(this.node.getName());
        JSONObject definitionProperties = (JSONObject) definitionElements.get("properties");
        JSONObject fieldObjects = new JSONObject();
        JSONObject properties = new JSONObject();
        JSONObject propertyElements = new JSONObject();


        for(GraphQLOutputType objectType : ((GraphQLUnionType)node).getTypes()){
            propertyElements.put("type", "null");
            properties.put(objectType.getName(), propertyElements);
        }

        fieldObjects.put("properties", properties);
        fieldObjects.put("type", "object");

        definitionProperties.put("_link_property_" + visitedFieldName, fieldObjects);

        return TraversalControl.ABORT;
    }
}
