package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeVisitorStub;
import graphql.schema.GraphQLUnionType;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLDirective.newDirective;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;

public class GraphQLQuerySearchVisitor extends GraphQLTypeVisitorStub {

    private static final Logger log = LoggerFactory.getLogger(GraphQLPaginationVisitor.class);

    private final GraphQLObjectType.Builder query;
    private final Map<String, GraphQLType> typeMap;
    private final GraphQLEnumType.Builder typeFilterEnum;
    private final GraphQLUnionType.Builder searchResultType;

    public GraphQLQuerySearchVisitor(Map<String, GraphQLType> typeMap, GraphQLObjectType query) {
        this.typeMap = typeMap;
        this.query = GraphQLObjectType.newObject(query);
        // Create a union type for the search results and a Type filter for the query.
        this.typeFilterEnum = GraphQLEnumType.newEnum()
                .name("TypeFilters")
                .description("Defines valid type filters");

        this.searchResultType = GraphQLUnionType.newUnionType()
                .name("SearchResult")
                .description("Union type for possible search results");
    }

    public GraphQLQuerySearchVisitor(Map<String, GraphQLType> typeMap) {
        this(typeMap, GraphQLObjectType.newObject().name("Query").build());
    }

    private static boolean isSearchable(GraphQLObjectType node) {
        for (GraphQLDirective directive : node.getDirectives()) {
            if ("domain".equals(directive.getName())) {
                GraphQLArgument searchable = directive.getArgument("searchable");
                return searchable != null && searchable.getValue() != null && searchable.getValue().equals(true);
            }
        }
        return false;
    }

    @Override
    public TraversalControl visitGraphQLObjectType(GraphQLObjectType node, TraverserContext<GraphQLType> context) {
        if (isSearchable(node)) {
            typeFilterEnum.value(node.getName());
            searchResultType.possibleType(node);
        }
        return TraversalControl.CONTINUE;
    }

    public GraphQLType getQuery() {

        if (typeMap.containsKey("TypeFilters")) {
            throw new IllegalArgumentException("type map already contains TypeFilters");
        }
        GraphQLEnumType typeFilterEnum = this.typeFilterEnum.build();
        typeMap.put("TypeFilters", typeFilterEnum);

        if (typeMap.containsKey("SearchResult")) {
            throw new IllegalArgumentException("type map already contains SearchResult");
        }
        GraphQLUnionType searchResultType = this.searchResultType.build();
        typeMap.put("SearchResult", searchResultType);

        // Create a new field in the query
        GraphQLFieldDefinition.Builder searchField = newFieldDefinition()
                .name("Search")
                .withDirective(newDirective().name("search").build())
                .argument(newArgument()
                        .name("query")
                        .type(nonNull(GraphQLString)))
                .argument(newArgument().name("filter")
                        .type(list(typeFilterEnum))
                        .build())
                .type(nonNull(list(nonNull(searchResultType))));

        query.field(searchField);

        return query.build();
    }
}
