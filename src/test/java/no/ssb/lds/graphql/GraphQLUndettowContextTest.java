package no.ssb.lds.graphql;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import static no.ssb.lds.graphql.GraphQLUndertowContext.SNAPSHOT_QUERY_NAME;
import static no.ssb.lds.graphql.GraphQLUndertowContext.SNAPSHOT_VARIABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GraphQLUndettowContextTest {

    private ZonedDateTime defaultSnapshot;

    @BeforeMethod
    public void setUp() {
        defaultSnapshot = ZonedDateTime.parse("2000-01-01T00:00:00Z");
        GraphQLUndertowContext.CLOCK = Clock.fixed(defaultSnapshot.toInstant(), defaultSnapshot.getZone());
    }

    @Test
    public void testVariable() {
        ZonedDateTime givenSnapshot = ZonedDateTime.parse("1999-01-01T00:00:00Z");
        Map<String, Object> variables = Map.of(SNAPSHOT_VARIABLE_NAME, givenSnapshot.toString());
        ZonedDateTime snapshot = GraphQLUndertowContext.getSnapshot(Collections.emptyMap(), variables);

        assertThat(snapshot).isEqualTo(givenSnapshot);
    }

    @Test
    public void testNullVariable() {
        LinkedHashMap<String, Object> variables = new LinkedHashMap<>();
        variables.put(SNAPSHOT_VARIABLE_NAME, null);
        ZonedDateTime snapshot = GraphQLUndertowContext.getSnapshot(Collections.emptyMap(), variables);

        assertThat(snapshot).isEqualTo(defaultSnapshot);
    }

    @Test
    public void testInvalidFormatVariable() {
        Map<String, Object> variables = Map.of(SNAPSHOT_VARIABLE_NAME, "not-a-date-time");

        ZonedDateTime snapshot = GraphQLUndertowContext.getSnapshot(Collections.emptyMap(), variables);
        assertThat(snapshot).isEqualTo(defaultSnapshot);
    }

    @Test
    public void testWrongTypeVariable() {
        Map<String, Object> variables = Map.of(SNAPSHOT_VARIABLE_NAME, false);

        assertThatThrownBy(() -> {
            GraphQLUndertowContext.getSnapshot(Collections.emptyMap(), variables);
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testQueryParameter() {
        ZonedDateTime givenSnapshot = ZonedDateTime.parse("2000-01-02T00:00:00Z");
        LinkedList<String> parameters = new LinkedList<>();
        parameters.add(givenSnapshot.toString());
        Map<String, Deque<String>> queryParameters = Map.of(SNAPSHOT_QUERY_NAME, parameters);
        ZonedDateTime snapshot = GraphQLUndertowContext.getSnapshot(queryParameters, Collections.emptyMap());

        assertThat(snapshot).isEqualTo(givenSnapshot);
    }

    @Test
    public void testMoreThanOneQueryParameter() {
        LinkedList<String> parameters = new LinkedList<>();
        parameters.add("not-a-date-time");
        parameters.add("2000-01-03T00:00:00Z");
        Map<String, Deque<String>> queryParameters = Map.of(SNAPSHOT_QUERY_NAME, parameters);

        assertThatThrownBy(() -> {
            GraphQLUndertowContext.getSnapshot(queryParameters, Collections.emptyMap());
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testNullQueryParameter() {
        LinkedList<String> parameters = new LinkedList<>();
        parameters.add(null);
        Map<String, Deque<String>> queryParameters = Map.of(SNAPSHOT_QUERY_NAME, parameters);
        ZonedDateTime snapshot = GraphQLUndertowContext.getSnapshot(queryParameters, Collections.emptyMap());

        assertThat(snapshot).isEqualTo(defaultSnapshot);
    }

    @Test
    public void testInvalidQueryParameter() {
        LinkedList<String> parameters = new LinkedList<>();
        parameters.add("not-a-variable");
        Map<String, Deque<String>> queryParameters = Map.of(SNAPSHOT_QUERY_NAME, parameters);
        ZonedDateTime snapshot = GraphQLUndertowContext.getSnapshot(queryParameters, Collections.emptyMap());

        assertThat(snapshot).isEqualTo(defaultSnapshot);
    }

    @Test
    public void testNoQueryParameterNoVariable() {
        ZonedDateTime snapshot =
                GraphQLUndertowContext.getSnapshot(Collections.emptyMap(), Collections.emptyMap());

        assertThat(snapshot).isEqualTo(defaultSnapshot);
    }

    @Test
    public void testQueryParameterAndVariable() {
        ZonedDateTime parameter = ZonedDateTime.parse("1999-01-01T00:00:00Z");
        ZonedDateTime variable = ZonedDateTime.parse("2001-01-01T00:00:00Z");
        LinkedList<String> parameters = new LinkedList<>();
        parameters.add(parameter.toString());
        Map<String, Deque<String>> queryParameters = Map.of(SNAPSHOT_QUERY_NAME, parameters);
        Map<String, Object> variables = Map.of(SNAPSHOT_VARIABLE_NAME, variable.toString());

        ZonedDateTime snapshot =
                GraphQLUndertowContext.getSnapshot(queryParameters, variables);

        assertThat(snapshot).isEqualTo(parameter);
    }
}