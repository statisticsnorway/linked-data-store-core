package no.ssb.lds.graphql.directives;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLNonNull;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLString;
import static graphql.introspection.Introspection.DirectiveLocation;

public class ReverseLinkDirective extends GraphQLDirective {

    public static final String NAME = "reverseLink";
    public static final String DESCRIPTION = "Defines a field as a reverse link to another managed domain";
    public static final String PAGINATION_NAME = "pagination";
    public static final String MAPPED_BY_NAME = "mappedBy";

    public static final GraphQLArgument PAGINATION_ARGUMENT = GraphQLArgument.newArgument()
            .name(PAGINATION_NAME)
            .type(GraphQLBoolean)
            .defaultValue(true)
            .build();

    public static final GraphQLArgument MAPPED_BY_ARGUMENT = GraphQLArgument.newArgument()
            .name(MAPPED_BY_NAME)
            .type(GraphQLString)
            .build();

    public static final ReverseLinkDirective INSTANCE = new ReverseLinkDirective(
            NAME,
            DESCRIPTION,
            EnumSet.of(DirectiveLocation.FIELD_DEFINITION),
            List.of(PAGINATION_ARGUMENT, MAPPED_BY_ARGUMENT)
    );

    public static final GraphQLDirective REVERSE_LINK_DIRECTIVE = GraphQLDirective.newDirective()
            .name("reverseLink")
            .argument(GraphQLArgument.newArgument().name("mappedBy").type(GraphQLNonNull.nonNull(GraphQLString)).build())
            .argument(GraphQLArgument.newArgument().name("pagination").type(GraphQLBoolean).build())
            .validLocation(DirectiveLocation.FIELD_DEFINITION)
            .build();

    private ReverseLinkDirective(String name, String description, EnumSet<DirectiveLocation> locations, List<GraphQLArgument> arguments) {
        super(name, description, locations, arguments);
    }

    public static ReverseLinkDirective newReverseLinkDirective(Boolean pagination, String mappedBy) {
        Objects.requireNonNull(mappedBy);
        Objects.requireNonNull(mappedBy);
        return new ReverseLinkDirective(
                NAME,
                DESCRIPTION,
                EnumSet.of(DirectiveLocation.FIELD_DEFINITION),
                List.of(
                        MAPPED_BY_ARGUMENT.transform(builder -> builder.value(mappedBy)),
                        PAGINATION_ARGUMENT.transform(builder -> builder.value(pagination)))
        );
    }

    public ReverseLinkDirective newReverseLinkDirective(String mappedBy) {
        return new ReverseLinkDirective(
                NAME,
                DESCRIPTION,
                EnumSet.of(DirectiveLocation.FIELD_DEFINITION),
                List.of(MAPPED_BY_ARGUMENT.transform(builder -> builder.value(mappedBy)))
        );
    }

    public String getMappedBy() {
        GraphQLArgument argument = getArgument(MAPPED_BY_NAME);
        return (String) argument.getValue();
    }

    public Boolean getPagination() {
        GraphQLArgument pagination = getArgument(PAGINATION_NAME);
        if (pagination.getValue() == null) {
            return (Boolean) pagination.getDefaultValue();
        }
        return (Boolean) pagination.getDefaultValue();
    }
}
