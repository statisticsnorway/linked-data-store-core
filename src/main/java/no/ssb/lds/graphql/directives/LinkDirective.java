package no.ssb.lds.graphql.directives;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLString;
import static graphql.introspection.Introspection.DirectiveLocation;

public class LinkDirective extends GraphQLDirective {

    public static final String NAME = "link";
    public static final String DESCRIPTION = "Defines a field as a link to another managed domain";
    public static final String PAGINATION_NAME = "pagination";
    public static final String REVERSE_NAME_NAME = "reverseName";
    public static final GraphQLArgument PAGINATION_ARGUMENT = GraphQLArgument.newArgument()
            .name(PAGINATION_NAME)
            .type(GraphQLBoolean)
            .defaultValue(true)
            .build();
    public static final GraphQLArgument REVERSE_NAME_ARGUMENT = GraphQLArgument.newArgument()
            .name(REVERSE_NAME_NAME)
            .type(GraphQLString)
            .build();

    public static final LinkDirective INSTANCE = new LinkDirective(
            NAME,
            DESCRIPTION,
            EnumSet.of(DirectiveLocation.FIELD_DEFINITION),
            List.of(PAGINATION_ARGUMENT, REVERSE_NAME_ARGUMENT),
            false, false, false
    );

    private LinkDirective(String name, String description, EnumSet<DirectiveLocation> locations, List<GraphQLArgument> arguments, boolean onOperation, boolean onFragment, boolean onField) {
        super(name, description, locations, arguments, onOperation, onFragment, onField);
    }

    public static LinkDirective newLinkDirective() {
        return newLinkDirective(true);
    }

    public static LinkDirective newLinkDirective(Boolean pagination) {
        return new LinkDirective(
                NAME,
                DESCRIPTION,
                EnumSet.of(DirectiveLocation.FIELD_DEFINITION),
                List.of(
                        PAGINATION_ARGUMENT.transform(builder -> builder.value(pagination))
                ), false, false, false
        );
    }

    public static LinkDirective newLinkDirective(Boolean pagination, String reverseName) {
        return new LinkDirective(
                NAME,
                DESCRIPTION,
                EnumSet.of(DirectiveLocation.FIELD_DEFINITION),
                List.of(
                        PAGINATION_ARGUMENT.transform(builder -> builder.value(pagination)),
                        REVERSE_NAME_ARGUMENT.transform(builder -> builder.value(reverseName))
                ), false, false, false
        );
    }

    public Optional<String> getReverseName() {
        GraphQLArgument argument = getArgument(REVERSE_NAME_NAME);
        if (argument == null || argument.getValue() == null) {
            return Optional.empty();
        } else {
            return Optional.of((String) argument.getValue());
        }
    }

    public Boolean getPagination() {
        GraphQLArgument pagination = getArgument(PAGINATION_NAME);
        if (pagination.getValue() == null) {
            return (Boolean) pagination.getDefaultValue();
        }
        return (Boolean) pagination.getDefaultValue();
    }
}
