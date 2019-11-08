
package no.ssb.lds.graphql.jsonSchema.visitors;

import graphql.schema.*;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static no.ssb.lds.graphql.directives.DomainDirective.hasDomainDirective;

public class ParseObjectTypesVisitor extends GraphQLTypeVisitorStub {
    private static final Logger LOG = LoggerFactory.getLogger(ParseObjectTypesVisitor.class);
    private static final TypeTraverser TRAVERSER = new TypeTraverser();
    private final Map<String, GraphQLType> typeMap;
    private final LinkedHashMap<String, JSONObject> jsonMap;
    private String visitedDefinition;

    public ParseObjectTypesVisitor(Map<String, GraphQLType> typeMap, LinkedHashMap<String, JSONObject> jsonMap) {
        this.typeMap = typeMap;
        this.jsonMap = jsonMap;
    }

    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        JSONObject jsonElements = new JSONObject();
        JSONObject jsonDefinitions = new JSONObject();
        JSONObject definitionValues = new JSONObject();

        if (hasDomainDirective(node)) {
            definitionValues.put("type", "object");
            definitionValues.put("properties", new JSONObject());
            definitionValues.put("required", new ArrayList<>());
            definitionValues.put("description", node.getDescription());
            definitionValues.put("displayName", "");

            jsonDefinitions.put(node.getName(), definitionValues);
            jsonElements.put("$ref", "#/definitions/" + node.getName());
            jsonElements.put("definitions", jsonDefinitions);
            jsonElements.put("#schema", "http://json-schema.org/draft-04/schema#");

            visitedDefinition = node.getName();

            AddDefinitionVisitor addJsonDefinitionVisitor = new AddDefinitionVisitor(jsonElements, visitedDefinition);
            TRAVERSER.depthFirst(addJsonDefinitionVisitor, node.getFieldDefinitions());
            jsonMap.put(node.getName(), jsonElements);

        }
        return TraversalControl.ABORT;
    }
}

