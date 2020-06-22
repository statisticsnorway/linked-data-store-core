package no.ssb.lds.graphql.jsonSchema.visitors;

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

import static no.ssb.lds.graphql.directives.DomainDirective.hasDomainDirective;


public class AddDefinitionVisitor extends GraphQLTypeVisitorStub {
    private static final Logger LOG = LoggerFactory.getLogger(AddDefinitionVisitor.class);
    private static final TypeTraverser TRAVERSER = new TypeTraverser();

    private final JSONObject jsonElements;
    private String visitedFieldName;
    private final String visitedDefinition;
    private String lastVisitedNodeType;


    public AddDefinitionVisitor(JSONObject jsonElements, String visitedDefinition) {
        this.jsonElements = jsonElements;
        this.visitedDefinition = visitedDefinition;
    }

    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {

        List<GraphQLDirective> directives = node.getDirectives();
        boolean isDomainType = directives.stream().map(GraphQLDirective::getName).anyMatch("domain"::equalsIgnoreCase);

        // Add objects in reference list for non-domain types.
        if (isDomainType) {
            return visitGraphQLDomainType(node, context);
        } else {
            JSONObject definitionElements = new JSONObject();
            definitionElements.put("type", "object");
            definitionElements.put("properties", new JSONObject());
            definitionElements.put("required", new ArrayList<>());

            JSONObject definitions = (JSONObject) jsonElements.get("definitions");
            definitions.put(node.getName(), definitionElements);

            AddDefinitionVisitor addReferencedDefinition = new AddDefinitionVisitor(jsonElements, node.getName());
            List<GraphQLFieldDefinition> graphQLFieldDefinitions = node.getFieldDefinitions();
            TRAVERSER.depthFirst(addReferencedDefinition, graphQLFieldDefinitions);

            return TraversalControl.ABORT;
        }
    }

    @Override
    public TraversalControl visitGraphQLFieldDefinition(GraphQLFieldDefinition node, TraverserContext<GraphQLType> context) {
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(visitedDefinition);
        JSONObject definitionProperties = (JSONObject) ((JSONObject) definitionElements.get("properties"));

        JSONObject propertyElements = new JSONObject();

        GraphQLOutputType type = node.getType();
        visitedFieldName = node.getName();

        lastVisitedNodeType = GraphQLFieldDefinition.class.getName();

        // add non-null type is the required list
        if (GraphQLTypeUtil.isNonNull(type)) {
            JSONArray defRequiredProperties = (JSONArray) definitionElements.get("required");
            defRequiredProperties.put(node.getName());
        }

        // handle scalar types(string, date-time)
        GraphQLType graphQLType = GraphQLTypeUtil.unwrapOne(node.getType());
        if (GraphQLTypeUtil.isScalar(graphQLType)) {
            if (graphQLType.getName().equalsIgnoreCase("string")) {
                propertyElements.put("type", "string");
            }
            switch (graphQLType.getName()) {
                case "String":
                    propertyElements.put("type", "string");
                    break;
                case "Boolean":
                    propertyElements.put("type", "boolean");
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

        // handle link types
        for (GraphQLDirective directive : node.getDirectives()) {
            if (directive.getName().equals(LinkDirective.NAME)) {
                GraphQLType subType;
                if (GraphQLTypeUtil.isNonNull(node.getType())) {
                    subType = GraphQLTypeUtil.unwrapNonNull(node.getType());
                } else {
                    subType = node.getType();
                }
                if (subType instanceof GraphQLUnionType) {
                    JSONObject properties = (JSONObject) ((JSONObject) definitionElements.get("properties")).get(visitedFieldName);
                    properties.put("type", "string");
                    visitUnionLinkProperty(node.getType(), context);
                    return TraversalControl.ABORT;
                } else if (subType instanceof GraphQLList) {
                    GraphQLType wrappedType = ((GraphQLList) subType).getWrappedType();
                    if (wrappedType instanceof GraphQLUnionType) {
                        JSONObject properties = (JSONObject) ((JSONObject) definitionElements.get("properties")).get(visitedFieldName);
                        JSONObject itemList = new JSONObject();
                        properties.put("type", "array");
                        itemList.put("type", "string");
                        properties.put("items", itemList);
                        visitUnionLinkProperty(wrappedType, context);
                        return TraversalControl.ABORT;
                    } else if (wrappedType instanceof GraphQLObjectType) {
                        visitListLinkProperty(wrappedType, context);
                        return TraversalControl.ABORT;
                    }
                } else if (subType instanceof GraphQLObjectType) {
                    visitGraphQLDomainType((GraphQLObjectType) subType, context);
                    return TraversalControl.ABORT;
                }
            }
        }

        return visitGraphQLType(node.getType(), context);
    }

    @Override
    public TraversalControl visitGraphQLEnumType(GraphQLEnumType node, TraverserContext<GraphQLType> context) {
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(visitedDefinition);
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
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(visitedDefinition);
        JSONObject definitionProperties = (JSONObject) definitionElements.get("properties");

        JSONObject fieldProperties = (JSONObject) definitionProperties.get(visitedFieldName);
        fieldProperties.put("type", "array");

        JSONObject itemList = new JSONObject();

        GraphQLType type;
        boolean isEmbeddedType = false;

        if (GraphQLTypeUtil.isNonNull(node.getWrappedType())) {
            type = ((GraphQLNonNull) node.getWrappedType()).getWrappedType();
        } else {
            type = node.getWrappedType();
        }

        if (type instanceof GraphQLObjectType) {
            if (!hasDomainDirective((GraphQLDirectiveContainer) type)) {
                itemList.put("$ref", "#/definitions/" + type.getName());
                fieldProperties.put("items", itemList);
                lastVisitedNodeType = GraphQLList.class.getName();

                return visitGraphQLType(node, context);
            } else {
                for (GraphQLFieldDefinition def : ((GraphQLObjectType) type).getFieldDefinitions()) {
                    GraphQLUnmodifiedType graphQLUnmodifiedType = GraphQLTypeUtil.unwrapAll(def.getType());
                    if (hasDomainDirective(def) || hasDomainDirective((GraphQLDirectiveContainer) graphQLUnmodifiedType)) {
                        isEmbeddedType = true;
                    }
                }
                if (isEmbeddedType) {
                    fieldProperties.put("type", "object");
                    visitEmbeddedType(node, context);
                    return TraversalControl.ABORT;
                }

                if (hasDomainDirective((GraphQLObjectType) type)) {
                    if (type instanceof GraphQLUnionType) {
                        visitUnionLinkProperty(type, context);
                        return TraversalControl.ABORT;
                    } else {
                        visitListLinkProperty(type, context);
                        return TraversalControl.ABORT;
                    }
                }
            }
        } else if (type instanceof GraphQLScalarType) {
            if (type.getName().equalsIgnoreCase("string")) {
                itemList.put("type", "string");
            }
            fieldProperties.put("items", itemList);
            lastVisitedNodeType = GraphQLList.class.getName();

            return visitGraphQLType(node, context);
        }

        return TraversalControl.CONTINUE;
    }


    @Override
    public TraversalControl visitGraphQLScalarType(GraphQLScalarType node, TraverserContext<GraphQLType> context) {
        return visitGraphQLType(node, context);
    }

    @Override
    public TraversalControl visitGraphQLNonNull(GraphQLNonNull node, TraverserContext<GraphQLType> context) {
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(visitedDefinition);
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

    private TraversalControl visitGraphQLDomainType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(visitedDefinition);
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

    private TraversalControl visitListLinkProperty(GraphQLType node, TraverserContext<GraphQLType> context) {
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(visitedDefinition);
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

        propertyElements.put("type", "null");

        if (GraphQLTypeUtil.isWrapped(node) && GraphQLTypeUtil.isList(node)) {
            properties.put(((GraphQLList) node).getWrappedType().getName(), propertyElements);
        } else if (GraphQLTypeUtil.isWrapped(node) && GraphQLTypeUtil.isNonNull(node)) {
            properties.put(((GraphQLNonNull) node).getWrappedType().getName(), propertyElements);
        } else {
            properties.put(node.getName(), propertyElements);
        }

        fieldObjects.put("properties", properties);
        fieldObjects.put("type", "object");

        definitionsProperties.put("_link_property_" + visitedFieldName, fieldObjects);

        return TraversalControl.ABORT;
    }

    private TraversalControl visitUnionLinkProperty(GraphQLType node, TraverserContext<GraphQLType> context) {
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(visitedDefinition);
        JSONObject definitionsProperties = (JSONObject) definitionElements.get("properties");

        JSONObject fieldObjects = new JSONObject();
        JSONObject properties = new JSONObject();
        JSONObject propertyElements = new JSONObject();

        for (GraphQLOutputType objectType : ((GraphQLUnionType) GraphQLTypeUtil.unwrapNonNull(node)).getTypes()) {
            propertyElements.put("type", "null");
            properties.put(objectType.getName(), propertyElements);
        }

        fieldObjects.put("properties", properties);
        fieldObjects.put("type", "object");

        definitionsProperties.put("_link_property_" + visitedFieldName, fieldObjects);

        return TraversalControl.ABORT;
    }

    private TraversalControl visitEmbeddedType(GraphQLList node, TraverserContext<GraphQLType> context) {
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(visitedDefinition);
        JSONObject definitionsProperties = (JSONObject) definitionElements.get("properties");
        JSONObject definitionProperties = (JSONObject) definitionsProperties.get(visitedFieldName);
        JSONObject embeddedProperties = new JSONObject();

        if (GraphQLTypeUtil.isList(node)) {
            GraphQLType graphQLType = GraphQLTypeUtil.unwrapOne(node);
            if (graphQLType instanceof GraphQLObjectType) {
                ((GraphQLObjectType) graphQLType).getFieldDefinitions().forEach(fieldDef -> {
                    System.out.println(fieldDef);
                    if (GraphQLTypeUtil.isList(fieldDef.getType())) {
                        JSONObject propertiesValues = new JSONObject();
                        JSONObject itemList = new JSONObject();

                        propertiesValues.put("type", "array");
                        itemList.put("type", "string");
                        propertiesValues.put("items", itemList);

                        embeddedProperties.put(fieldDef.getName(), propertiesValues);

                        JSONObject linkedProperties = new JSONObject();
                        JSONObject linkedObjects = new JSONObject();

                        linkedProperties.put("type", "object");
                        propertiesValues = new JSONObject();
                        linkedObjects.put("type", "null");
                        propertiesValues.put(((GraphQLList) fieldDef.getType()).getWrappedType().getName(), linkedObjects);
                        linkedProperties.put("properties", propertiesValues);

                        embeddedProperties.put("_link_property_" + fieldDef.getName(), linkedProperties);
                    }
                });
            }

        }
        definitionProperties.put("properties", embeddedProperties);
        System.out.println(definitionProperties);

        return TraversalControl.ABORT;
    }
}
