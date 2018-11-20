package no.ssb.lds.graphql;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import no.ssb.lds.api.persistence.Persistence;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PersistenceLinkFetcher implements DataFetcher<Map<String, Object>> {

    private final String field;
    private final String target;
    private final Persistence persistence;
    private final Pattern pattern;
    private final String namespace;

    public PersistenceLinkFetcher(Persistence persistence, String namespace, String field, String target) {
        this.field = field;
        this.target = target;
        this.persistence = persistence;
        this.pattern = Pattern.compile("/" + target + "/(?<id>.*)");
        this.namespace = namespace;
    }

    @Override
    public Map<String, Object> get(DataFetchingEnvironment environment) throws Exception {
        Map<String, Object> source = environment.getSource();
        String link = (String) source.get(field);
        Matcher matcher = pattern.matcher(link);
        if (matcher.matches()) {
            String id = matcher.group("id");
            return persistence.read(namespace, target, id).toMap();
        }
        return null;
    }
}
