package no.ssb.lds.graphql;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import no.ssb.lds.api.persistence.Persistence;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PersistenceLinksFetcher implements DataFetcher<List<Map<String, Object>>> {

    private final String field;
    private final String target;
    private final Persistence persistence;
    private final Pattern pattern;
    private final String namespace;

    public PersistenceLinksFetcher(Persistence persistence, String namespace, String field, String target) {
        this.field = field;
        this.target = target;
        this.persistence = persistence;
        this.pattern = Pattern.compile("/" + target + "/(?<id>.*)");
        this.namespace = namespace;
    }

    @Override
    public List<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
        Map<String, Object> source = environment.getSource();
        List<String> links = (List<String>) source.get(field);
        List<Map<String, Object>> results = new ArrayList<>();
        if (links == null) {
            return null;
        }
        for (String link : links) {
            Matcher matcher = pattern.matcher(link);
            if (matcher.matches()) {
                String id = matcher.group("id");
                JSONObject entity = persistence.read(namespace, target, id);
                results.add(entity != null ? entity.toMap() : null);
            } else {
                // TODO: Handle.
            }
        }
        return results;
    }
}
