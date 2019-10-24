package no.ssb.lds.graphql.jsonSchema.visitors;

import graphql.schema.*;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class AddDefinitionVisitor extends GraphQLTypeVisitorStub {
    private static final Logger LOG = LoggerFactory.getLogger(AddDefinitionVisitor.class);
    private static final TypeTraverser TRAVERSER = new TypeTraverser();

    private GraphQLObjectType node;
    private JSONObject jsonElements;

    private String visitedFieldName;

    public AddDefinitionVisitor(GraphQLObjectType node, JSONObject jsonElements) {
        this.node = node;
        this.jsonElements = jsonElements;
    }

    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        // LOG.info("Parsing [{}]-->[{}]", this.node.getName(), node.getName());
        if ((node.getName().indexOf("Connection") != -1)) {
            return visitGraphQLConnection(node, context);
        } else {
            List<GraphQLDirective> directives = node.getDirectives();
            boolean isDomainType = directives.stream().map(GraphQLDirective::getName).anyMatch("domain"::equalsIgnoreCase);

            if(isDomainType){
                return visitGraphQLTypeWithDomain(node, context);
            }else{
                JSONObject definitionElements = new JSONObject();
                definitionElements.put("type", "object");
                definitionElements.put("properties", new JSONObject());
                definitionElements.put("required", new ArrayList<>());

                JSONObject definitions = (JSONObject) jsonElements.get("definitions");
                definitions.put(node.getName(), definitionElements);

                AddReferenceDefinitionVisitor addReferenceDefinitionVisitor = new AddReferenceDefinitionVisitor(node, jsonElements);
                TRAVERSER.depthFirst(addReferenceDefinitionVisitor, node.getFieldDefinitions());

                return TraversalControl.ABORT;
            }
        }
    }

    @Override
    public TraversalControl visitGraphQLFieldDefinition(GraphQLFieldDefinition node, TraverserContext<GraphQLType> context) {
        //LOG.info("Parsing field:: [{}]-->[{}]", this.node.getName(), node.getName());

        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(this.node.getName());
        JSONObject definitionProperties = (JSONObject) definitionElements.get("properties");
        JSONObject propertyElements = new JSONObject();

        propertyElements.put("description", node.getDescription());
        propertyElements.put("displayName", "");

        definitionProperties.put(node.getName(), propertyElements);

        visitedFieldName = node.getName();

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

        return visitGraphQLType(node, context);
    }

    @Override
    public TraversalControl visitGraphQLScalarType(GraphQLScalarType node, TraverserContext<GraphQLType> context) {
        //LOG.info("Parsing scalar:: [{}]-->[{}]", this.node.getName(), visitedFieldName);
        return visitGraphQLType(node, context);
    }

    @Override
    public TraversalControl visitGraphQLNonNull(GraphQLNonNull node, TraverserContext<GraphQLType> context) {
        //LOG.info("Parsing non-null:: [{}]-->[{}]", this.node.getName(), visitedFieldName);
        return visitGraphQLType(node, context);
    }

    public TraversalControl visitGraphQLConnection(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        //LOG.info("Parsing connection:: [{}]-->[{}]", this.node.getName(), visitedFieldName);

        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(this.node.getName());
        JSONObject definitionProperties = (JSONObject) definitionElements.get("properties");

        JSONObject fieldProperties = (JSONObject) definitionProperties.get(visitedFieldName);
        fieldProperties.put("type", "array");

        JSONObject itemList = new JSONObject();
        itemList.put("type", "string");

        fieldProperties.put("items", itemList);

        return TraversalControl.ABORT;
    }

    public TraversalControl visitGraphQLTypeWithDomain(GraphQLObjectType node, TraverserContext<GraphQLType> context){
        JSONObject definitionElements = (JSONObject) ((JSONObject) jsonElements.get("definitions")).get(this.node.getName());
        JSONObject definitionProperties = (JSONObject) definitionElements.get("properties");

        JSONObject fieldProperties = (JSONObject) definitionProperties.get(visitedFieldName);
        fieldProperties.put("type", "string");
        fieldProperties.put("displayName", "");
        fieldProperties.put("description", node.getDescription());

        return TraversalControl.ABORT;
    }


}
