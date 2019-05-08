package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.GraphQLTypeUtil;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Stack;

/**
 * Given a typeMap this visitor will replace all types with type references.
 */
public class GraphQLTypeReferencerVisitor extends GraphQLTypeVisitorStub {

    private static final Logger log = LoggerFactory.getLogger(GraphQLTypeReferencerVisitor.class);
    private final Map<String, GraphQLType> typeMap;

    public GraphQLTypeReferencerVisitor(Map<String, GraphQLType> typeMap) {
        this.typeMap = Objects.requireNonNull(typeMap);
    }

    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        GraphQLObjectType.Builder newObject = GraphQLObjectType.newObject(node);
        for (GraphQLFieldDefinition fieldDefinition : node.getFieldDefinitions()) {
            Stack<GraphQLType> types = GraphQLTypeUtil.unwrapType(fieldDefinition.getType());
            GraphQLType current = types.pop();
            if (current instanceof GraphQLTypeReference) {
                log.debug("field {} of {} was already a reference", fieldDefinition.getName(), node.getName());
                break;
            }
            if (current instanceof GraphQLScalarType) {
                break;
            }
            if (!typeMap.containsKey(current.getName())) {
                throw new AssertionError("type was not in type map");
            }
            log.debug("replacing field {} with a reference to {}",
                    GraphQLTypeUtil.simplePrint(fieldDefinition.getType()), current.getName());
            GraphQLOutputType newType = GraphQLTypeReference.typeRef(current.getName());
            while (!types.empty()) {
                current = types.pop();
                if (GraphQLTypeUtil.isList(current)) {
                    newType = GraphQLList.list(newType);
                } else if (GraphQLTypeUtil.isNonNull(current)) {
                    newType = GraphQLNonNull.nonNull(newType);
                } else {
                    throw new AssertionError("non wrapped type up the stack");
                }
            }

            newObject.field(GraphQLFieldDefinition.newFieldDefinition(fieldDefinition).type(newType).build());
        }
        typeMap.put(node.getName(), newObject.build());
        return super.visitGraphQLObjectType(node, context);
    }
}
