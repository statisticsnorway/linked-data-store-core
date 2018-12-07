package no.ssb.lds.core.linkeddata;

import java.util.Objects;

public class OutgoingLink {

    public final String internalId;         // internal persistence specific id. Not used by LinkedData!
    public final String resourceURL;        // e.g. /data/provisionagreement/100
    public final String relationshipURI;    // e.g. /data/provisionagreement/100/support/otherSupport/contact/105
    public final String relationshipName;   // e.g. OTHER_SUPPORT_HAS_REF_TO
    public final String edgeResourceURL;    // e.g. /data/contact/105
    public final String namespace;          // e.g. data
    public final String entity;             // e.g. provisionagreement
    public final String id;                 // e.g. 100
    public final String edgeEntity;         // e.g. contact
    public final String edgeId;             // e.g. 105

    public OutgoingLink(String internalId, String relationshipURI, String relationshipName, String namespace, String entity, String id, String edgeEntity, String edgeId) {
        this.internalId = internalId;
        this.resourceURL = String.format("/%s/%s/%s", namespace, entity, id);
        this.relationshipURI = relationshipURI;
        this.relationshipName = relationshipName;
        this.edgeResourceURL = String.format("/%s/%s/%s", namespace, edgeEntity, edgeId);
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
        this.edgeEntity = edgeEntity;
        this.edgeId = edgeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutgoingLink that = (OutgoingLink) o;
        return Objects.equals(relationshipURI, that.relationshipURI);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relationshipURI);
    }

    @Override
    public String toString() {
        return "no.ssb.lds.domain.linkeddata.OutgoingLink{" +
                "internalId='" + internalId + '\'' +
                ", resourceURL='" + resourceURL + '\'' +
                ", relationshipURI='" + relationshipURI + '\'' +
                ", relationshipName='" + relationshipName + '\'' +
                ", edgeResourceURL='" + edgeResourceURL + '\'' +
                ", namespace='" + namespace + '\'' +
                ", entity='" + entity + '\'' +
                ", id='" + id + '\'' +
                ", edgeEntity='" + edgeEntity + '\'' +
                ", edgeId='" + edgeId + '\'' +
                '}';
    }
}
