package no.ssb.lds.graphqlneo4j;

import graphql.ExecutionInput;
import graphql.analysis.QueryTraversal;
import graphql.analysis.QueryVisitorFieldEnvironment;
import graphql.analysis.QueryVisitorStub;
import graphql.language.Argument;
import graphql.language.ArrayValue;
import graphql.language.BooleanValue;
import graphql.language.Document;
import graphql.language.Field;
import graphql.language.FloatValue;
import graphql.language.IntValue;
import graphql.language.Node;
import graphql.language.NodeTraverser;
import graphql.language.ObjectField;
import graphql.language.ObjectValue;
import graphql.language.StringValue;
import graphql.language.Value;
import graphql.language.VariableReference;
import graphql.parser.Parser;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLTypeUtil;
import graphql.schema.GraphQLUnmodifiedType;
import graphql.util.TraverserContext;

import java.util.ArrayList;
import java.util.List;

import static graphql.language.NodeTraverser.LeaveOrEnter.LEAVE;

public class GraphQLQueryTransformer {

    static String addTimeBasedVersioningArgumentValues(GraphQLSchema graphQlSchema, ExecutionInput executionInput) {
        Document document = new Parser().parseDocument(executionInput.getQuery());
        QueryTraversal queryTraversal = QueryTraversal.newQueryTraversal()
                .schema(graphQlSchema)
                .operationName(executionInput.getOperationName())
                .variables(executionInput.getVariables())
                .document(document)
                .build();
        StringBuilder sb = new StringBuilder("{\n");
        queryTraversal.visitDepthFirst(new QueryVisitorStub() {
            @Override
            public void visitField(QueryVisitorFieldEnvironment env) {
                TraverserContext<Node> context = env.getTraverserContext();
                int depth = context.getParentNodes().size();
                String indent = " ".repeat(depth);
                NodeTraverser.LeaveOrEnter leaveOrEnter = context.getVar(NodeTraverser.LeaveOrEnter.class);
                GraphQLUnmodifiedType fieldType = GraphQLTypeUtil.unwrapAll(env.getFieldDefinition().getType());
                if (leaveOrEnter == LEAVE) {
                    if (fieldType instanceof GraphQLScalarType ||
                            fieldType instanceof GraphQLEnumType) {
                    } else {
                        sb.append(indent).append("}\n");
                    }
                    return;
                }
                Field field = env.getField();
                sb.append(indent).append(field.getName());
                List<GraphQLArgument> schemaDefinedArguments = env.getFieldDefinition().getArguments();
                if (!schemaDefinedArguments.isEmpty()) {
                    sb.append("(");
                    int i = 0;
                    List<Argument> arguments = new ArrayList<>(field.getArguments());
                    env.getFieldDefinition().getArguments().stream()
                            .filter(a -> "ver".equals(a.getName()) && "_Neo4jDateTimeInput".equals(a.getType().getName()))
                            .findFirst()
                            .map(a -> Argument.newArgument()
                                    .name(a.getName())
                                    .value(VariableReference.newVariableReference()
                                            .name("_version")
                                            .build())
                                    .build())
                            .ifPresent(arguments::add);
                    for (Argument argument : arguments) {
                        if (i++ > 0) {
                            sb.append(", ");
                        }
                        sb.append(argument.getName()).append(": ");
                        Value value = argument.getValue();
                        serializeValue(sb, value);
                    }
                    sb.append(")");
                }
                if (fieldType instanceof GraphQLScalarType ||
                        fieldType instanceof GraphQLEnumType) {
                } else {
                    sb.append(" {");
                }
                sb.append("\n");
            }
        });
        sb.append("}\n");
        return sb.toString();
    }

    private static void serializeValue(StringBuilder sb, Value value) {
        if (value instanceof VariableReference) {
            sb.append("$").append(((VariableReference) value).getName());
        } else if (value instanceof BooleanValue) {
            sb.append(((BooleanValue) value).isValue());
        } else if (value instanceof FloatValue) {
            sb.append(((FloatValue) value).getValue());
        } else if (value instanceof IntValue) {
            sb.append(((IntValue) value).getValue());
        } else if (value instanceof StringValue) {
            sb.append("\"").append(((StringValue) value).getValue()).append("\"");
        } else if (value instanceof ObjectValue) {
            sb.append("{");
            int i = 0;
            for (ObjectField objectField : ((ObjectValue) value).getObjectFields()) {
                if (i++ > 0) {
                    sb.append(", ");
                }
                sb.append(objectField.getName()).append(": ");
                Value v = objectField.getValue();
                serializeValue(sb, v);
            }
            sb.append("}");
        } else if (value instanceof ArrayValue) {
            sb.append("[");
            for (Value v : ((ArrayValue) value).getValues()) {
                serializeValue(sb, v);
            }
            sb.append("]");
        } else {
            throw new IllegalArgumentException("Unsupported Value class: " + value.getClass().getName());
        }
    }
}
