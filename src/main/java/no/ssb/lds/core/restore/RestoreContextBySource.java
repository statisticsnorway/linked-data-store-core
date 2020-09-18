package no.ssb.lds.core.restore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RestoreContextBySource {

    final Map<String, RestoreContext> map = new ConcurrentHashMap<>();
}
