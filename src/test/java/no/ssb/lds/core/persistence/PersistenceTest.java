package no.ssb.lds.core.persistence;

import no.ssb.lds.api.persistence.OutgoingLink;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class PersistenceTest {

    @Test
    public void testPersistenceAdapter() {
        // persistence adapter
        Persistence cord = new Persistence() {
            @Override
            public boolean createOrOverwrite(String namespace, String entity, String id, JSONObject jsonObject, Set<OutgoingLink> links) throws PersistenceException {
                return true;
            }

            @Override
            public JSONObject read(String namespace, String entity, String id) throws PersistenceException {
                return null;
            }

            @Override
            public boolean delete(String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
                return false;
            }

            @Override
            public JSONArray findAll(String namespace, String entity) throws PersistenceException {
                return null;
            }

            @Override
            public void close() throws PersistenceException {

            }
        };

        // Get Managed Domain Spec

        String ns = "";
        Map<String, OutgoingLink> outgoingLinkMap = new LinkedHashMap<>();
        cord.createOrOverwrite(ns, null, null, null, null);
    }

}
