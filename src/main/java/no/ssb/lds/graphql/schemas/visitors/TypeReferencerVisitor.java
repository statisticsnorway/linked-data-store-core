package no.ssb.lds.graphql.schemas.visitors;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.GraphQLTypeUtil;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.schema.GraphQLUnionType;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;

/**
 * Given a typeMap this visitor will replace all types with type references.
 */
public class TypeReferencerVisitor extends GraphQLTypeVisitorStub {

    private static final Logger log = LoggerFactory.getLogger(TypeReferencerVisitor.class);
    private final Map<String, GraphQLType> typeMap;

    public TypeReferencerVisitor(Map<String, GraphQLType> typeMap) {
        this.typeMap = Objects.requireNonNull(typeMap);
    }

    @Override
    public TraversalControl visitGraphQLInterfaceType(GraphQLInterfaceType node, TraverserContext<GraphQLType> context) {
        GraphQLInterfaceType.Builder newInterface = GraphQLInterfaceType.newInterface(
                (GraphQLInterfaceType) typeMap.get(node.getName()));
        for (GraphQLFieldDefinition fieldDefinition : node.getFieldDefinitions()) {
            convertToReference(fieldDefinition.getType()).ifPresent(reference -> {
                newInterface.field(GraphQLFieldDefinition.newFieldDefinition(fieldDefinition).type(reference).build());
            });
        }

        // TODO: Figure out why this fails. We should have tree with same instance of all types
        //if (!typeMap.replace(existing.getName(), existing, newObject)) {
        //    throw new IllegalArgumentException(String.format(
        //            "Could not replace %s, the schema probably contains references", existing.getName()
        //    ));
        //}
        GraphQLType oldObject = typeMap.put(node.getName(), newInterface.build());
        if (oldObject != null && Objects.equals(oldObject, node)) {
            log.debug("Existing object {} is not equal to visited object {}", node, oldObject);
        }
        return TraversalControl.CONTINUE;
    }

    @Override
    public TraversalControl visitGraphQLUnionType(GraphQLUnionType node, TraverserContext<GraphQLType> context) {
        GraphQLUnionType.Builder newUnionType = GraphQLUnionType.newUnionType(
                (GraphQLUnionType) typeMap.get(node.getName()));
        for (GraphQLOutputType type : node.getTypes()) {
            if (!(type instanceof GraphQLTypeReference)) {
                newUnionType.possibleType(GraphQLTypeReference.typeRef(type.getName()));
            }
        }
        // TODO: Figure out why this fails. We should have tree with same instance of all types
        //if (!typeMap.replace(existing.getName(), existing, newObject)) {
        //    throw new IllegalArgumentException(String.format(
        //            "Could not replace %s, the schema probably contains references", existing.getName()
        //    ));
        //}
        GraphQLType oldObject = typeMap.put(node.getName(), newUnionType.build());
        if (oldObject != null && Objects.equals(oldObject, node)) {
            log.debug("Existing object {} is not equal to visited object {}", node, oldObject);
        }
        return TraversalControl.CONTINUE;
    }

    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        GraphQLObjectType.Builder newObject = GraphQLObjectType.newObject(
                (GraphQLObjectType) typeMap.get(node.getName()));
        for (GraphQLFieldDefinition fieldDefinition : node.getFieldDefinitions()) {
            convertToReference(fieldDefinition.getType()).ifPresent(reference -> {
                newObject.field(GraphQLFieldDefinition.newFieldDefinition(fieldDefinition).type(reference).build());
            });
        }
        for (GraphQLOutputType anInterface : node.getInterfaces()) {
            convertToReference(anInterface).ifPresent(reference -> {
                newObject.withInterfaces(GraphQLTypeReference.typeRef(reference.getName()));
            });
        }
        // TODO: Figure out why this fails. We should have tree with same instance of all types
        //if (!typeMap.replace(existing.getName(), existing, newObject)) {
        //    throw new IllegalArgumentException(String.format(
        //            "Could not replace %s, the schema probably contains references", existing.getName()
        //    ));
        //}
        GraphQLType oldObject = typeMap.put(node.getName(), newObject.build());
        if (oldObject != null && Objects.equals(oldObject, node)) {
            log.debug("Existing object {} is not equal to visited object {}", node, oldObject);
        }
        typeMap.put(node.getName(), newObject.build());
        return TraversalControl.CONTINUE;
    }

    private Optional<GraphQLOutputType> convertToReference(GraphQLType type) {
        Stack<GraphQLType> types = GraphQLTypeUtil.unwrapType(type);
        GraphQLType current = types.pop();
        if (current instanceof GraphQLTypeReference || current instanceof GraphQLScalarType) {
            return Optional.empty();
        }
        if (!typeMap.containsKey(current.getName())) {
            throw new AssertionError("type was not in type map");
        }
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
        return Optional.of(newType);
    }
}
