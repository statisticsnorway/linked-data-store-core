package no.ssb.lds.graphql.directives;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;

import java.util.EnumSet;
import java.util.List;

import static graphql.Scalars.GraphQLBoolean;
import static graphql.introspection.Introspection.DirectiveLocation;

public class DomainDirective extends GraphQLDirective {

    private static final String NAME = "domain";
    private static final String SEARCHABLE_NAME = "searchable";
    private static final GraphQLArgument SEARCHABLE_ARGUMENT = GraphQLArgument.newArgument()
            .name(SEARCHABLE_NAME)
            .type(GraphQLBoolean)
            .defaultValue(true)
            .build();
    private static final String DESCRIPTION = "Marks a type definition as a managed domain object";

    public static DomainDirective INSTANCE = new DomainDirective(
            NAME,
            DESCRIPTION,
            EnumSet.of(DirectiveLocation.OBJECT),
            List.of(SEARCHABLE_ARGUMENT),
            true, true, true
    );

    private DomainDirective(String name, String description, EnumSet<DirectiveLocation> locations, List<GraphQLArgument> arguments, boolean onOperation, boolean onFragment, boolean onField) {
        super(name, description, locations, arguments, onOperation, onFragment, onField);
    }

    public static DomainDirective newDomainDirective(Boolean searchable) {
        return new DomainDirective(
                NAME,
                DESCRIPTION,
                EnumSet.of(DirectiveLocation.OBJECT),
                List.of(SEARCHABLE_ARGUMENT.transform(builder -> builder.value(searchable))),
                true, true, true
        );
    }

    public Boolean isSearchable() {
        GraphQLArgument argument = getArgument(SEARCHABLE_NAME);
        Object value = argument.getValue();
        if (value != null) {
            return (Boolean) value;
        }
        return (Boolean) argument.getDefaultValue();
    }

}
