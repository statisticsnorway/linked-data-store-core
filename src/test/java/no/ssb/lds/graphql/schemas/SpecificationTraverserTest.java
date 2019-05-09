package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLType;
import graphql.schema.idl.SchemaPrinter;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElementType;
import no.ssb.lds.core.specification.SpecificationBuilder;
import no.ssb.lds.core.specification.TestSpecificationElement;
import org.testng.annotations.Test;

import java.util.Set;

import static no.ssb.lds.core.specification.SpecificationBuilder.arrayNode;
import static no.ssb.lds.core.specification.SpecificationBuilder.booleanNode;
import static no.ssb.lds.core.specification.SpecificationBuilder.createSpecificationAndRoot;
import static no.ssb.lds.core.specification.SpecificationBuilder.numericNode;
import static no.ssb.lds.core.specification.SpecificationBuilder.objectNode;
import static no.ssb.lds.core.specification.SpecificationBuilder.stringNode;
import static org.assertj.core.api.Assertions.assertThat;

public class SpecificationTraverserTest {

    @Test
    public void testReferences() {

        TestSpecificationElement leafObject = objectNode("LeafObject", Set.of(
                SpecificationBuilder.refNode("oneToOne", Set.of("OtherObject")),
                SpecificationBuilder.arrayRefNode("oneToMany", Set.of("OtherObject"))
        ));

        TestSpecificationElement embeddedObject = objectNode("EmbeddedObject", Set.of(
                stringNode("stringProperty"),
                leafObject
        ));

        TestSpecificationElement object = objectNode(SpecificationElementType.MANAGED, "Object", Set.of(
                stringNode("stringProperty"),
                embeddedObject
        ));

        TestSpecificationElement otherObject = objectNode(SpecificationElementType.MANAGED, "OtherObject", Set.of(
                stringNode("stringProperty")
        ));
        Specification specification = createSpecificationAndRoot(Set.of(
                object,
                embeddedObject,
                leafObject,
                otherObject
        ));
        SpecificationTraverser traverser = new SpecificationTraverser(specification);
        String result = serialize(traverser);
        assertThat(result).isEqualToIgnoringWhitespace("" +
                "#Description: TestSpecificationElement{path='$', Type=MANAGED, jsonTypes=[object]}" +
                "type OtherObject @domain(searchable : true) {" +
                "  #Description: TestSpecificationElement{path='$.stringProperty', Type=EMBEDDED, jsonTypes=[string]}" +
                "  stringProperty: String!" +
                "}" +
                "" +
                "#Description: TestSpecificationElement{path='$', Type=MANAGED, jsonTypes=[object]}" +
                "type Object @domain(searchable : true) {" +
                "  #Description: TestSpecificationElement{path='$.EmbeddedObject', Type=EMBEDDED, jsonTypes=[object]}" +
                "  EmbeddedObject: EmbeddedObject!" +
                "  #Description: TestSpecificationElement{path='$.stringProperty', Type=EMBEDDED, jsonTypes=[string]}" +
                "  stringProperty: String!" +
                "}" +
                "" +
                "#Description: TestSpecificationElement{path='$.EmbeddedObject', Type=EMBEDDED, jsonTypes=[object]}" +
                "type EmbeddedObject {" +
                "  #Description: TestSpecificationElement{path='$.LeafObject', Type=EMBEDDED, jsonTypes=[object]}" +
                "  LeafObject: LeafObject!" +
                "  #Description: TestSpecificationElement{path='$.EmbeddedObject.stringProperty', Type=EMBEDDED, jsonTypes=[string]}" +
                "  stringProperty: String!" +
                "}" +
                "" +
                "#Description: TestSpecificationElement{path='$.LeafObject', Type=EMBEDDED, jsonTypes=[object]}" +
                "type LeafObject {" +
                "  #Description: TestSpecificationElement{path='$.LeafObject.oneToMany', Type=REF, jsonTypes=[array]}" +
                "  oneToMany: [OtherObject]! @link(pagination : true)" +
                "  #Description: TestSpecificationElement{path='$.LeafObject.oneToOne', Type=REF, jsonTypes=[string]}" +
                "  oneToOne: OtherObject! @link(pagination : true)" +
                "}"
        );
    }

    @Test
    public void testTypes() {

        TestSpecificationElement leafObject = objectNode("LeafObject", Set.of(
                stringNode("stringProperty"),
                arrayNode("stringArrayProperty", stringNode("string")),
                numericNode("numericProperty"),
                arrayNode("numericArrayProperty", numericNode("numeric")),
                booleanNode("booleanProperty"),
                arrayNode("booleanArrayProperty", booleanNode("boolean"))
        ));

        TestSpecificationElement embeddedObject = objectNode("EmbeddedObject", Set.of(
                stringNode("stringProperty"),
                arrayNode("stringArrayProperty", stringNode("string")),
                numericNode("numericProperty"),
                arrayNode("numericArrayProperty", numericNode("numeric")),
                booleanNode("booleanProperty"),
                arrayNode("booleanArrayProperty", booleanNode("boolean")),
                leafObject
        ));

        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "Object", Set.of(
                        stringNode("stringProperty"),
                        arrayNode("stringArrayProperty", stringNode("string")),
                        numericNode("numericProperty"),
                        arrayNode("numericArrayProperty", numericNode("numeric")),
                        booleanNode("booleanProperty"),
                        arrayNode("booleanArrayProperty", booleanNode("boolean")),
                        embeddedObject
                )),
                embeddedObject,
                leafObject
        ));
        SpecificationTraverser traverser = new SpecificationTraverser(specification);

        String result = serialize(traverser);
        assertThat(result).isEqualToIgnoringWhitespace("" +
                "#Description: TestSpecificationElement{path='$.EmbeddedObject', Type=EMBEDDED, jsonTypes=[object]}" +
                "type EmbeddedObject {" +
                "  #Description: TestSpecificationElement{path='$.LeafObject', Type=EMBEDDED, jsonTypes=[object]}" +
                "  LeafObject: LeafObject!" +
                "  #Description: TestSpecificationElement{path='$.EmbeddedObject.booleanArrayProperty', Type=EMBEDDED, jsonTypes=[array]}" +
                "  booleanArrayProperty: [String]!" +
                "  #Description: TestSpecificationElement{path='$.EmbeddedObject.booleanProperty', Type=EMBEDDED, jsonTypes=[boolean]}" +
                "  booleanProperty: Boolean!" +
                "  #Description: TestSpecificationElement{path='$.EmbeddedObject.numericArrayProperty', Type=EMBEDDED, jsonTypes=[array]}" +
                "  numericArrayProperty: [String]!" +
                "  #Description: TestSpecificationElement{path='$.EmbeddedObject.numericProperty', Type=EMBEDDED, jsonTypes=[number]}" +
                "  numericProperty: Float!" +
                "  #Description: TestSpecificationElement{path='$.EmbeddedObject.stringArrayProperty', Type=EMBEDDED, jsonTypes=[array]}" +
                "  stringArrayProperty: [String]!" +
                "  #Description: TestSpecificationElement{path='$.EmbeddedObject.stringProperty', Type=EMBEDDED, jsonTypes=[string]}" +
                "  stringProperty: String!" +
                "}" +
                "" +
                "#Description: TestSpecificationElement{path='$', Type=MANAGED, jsonTypes=[object]}" +
                "type Object @domain(searchable : true) {" +
                "  #Description: TestSpecificationElement{path='$.EmbeddedObject', Type=EMBEDDED, jsonTypes=[object]}" +
                "  EmbeddedObject: EmbeddedObject!" +
                "  #Description: TestSpecificationElement{path='$.booleanArrayProperty', Type=EMBEDDED, jsonTypes=[array]}" +
                "  booleanArrayProperty: [String]!" +
                "  #Description: TestSpecificationElement{path='$.booleanProperty', Type=EMBEDDED, jsonTypes=[boolean]}" +
                "  booleanProperty: Boolean!" +
                "  #Description: TestSpecificationElement{path='$.numericArrayProperty', Type=EMBEDDED, jsonTypes=[array]}" +
                "  numericArrayProperty: [String]!" +
                "  #Description: TestSpecificationElement{path='$.numericProperty', Type=EMBEDDED, jsonTypes=[number]}" +
                "  numericProperty: Float!" +
                "  #Description: TestSpecificationElement{path='$.stringArrayProperty', Type=EMBEDDED, jsonTypes=[array]}" +
                "  stringArrayProperty: [String]!" +
                "  #Description: TestSpecificationElement{path='$.stringProperty', Type=EMBEDDED, jsonTypes=[string]}" +
                "  stringProperty: String!" +
                "}" +
                "" +
                "#Description: TestSpecificationElement{path='$.LeafObject', Type=EMBEDDED, jsonTypes=[object]}" +
                "type LeafObject {" +
                "  #Description: TestSpecificationElement{path='$.LeafObject.booleanArrayProperty', Type=EMBEDDED, jsonTypes=[array]}" +
                "  booleanArrayProperty: [String]!" +
                "  #Description: TestSpecificationElement{path='$.LeafObject.booleanProperty', Type=EMBEDDED, jsonTypes=[boolean]}" +
                "  booleanProperty: Boolean!" +
                "  #Description: TestSpecificationElement{path='$.LeafObject.numericArrayProperty', Type=EMBEDDED, jsonTypes=[array]}" +
                "  numericArrayProperty: [String]!" +
                "  #Description: TestSpecificationElement{path='$.LeafObject.numericProperty', Type=EMBEDDED, jsonTypes=[number]}" +
                "  numericProperty: Float!" +
                "  #Description: TestSpecificationElement{path='$.LeafObject.stringArrayProperty', Type=EMBEDDED, jsonTypes=[array]}" +
                "  stringArrayProperty: [String]!" +
                "  #Description: TestSpecificationElement{path='$.LeafObject.stringProperty', Type=EMBEDDED, jsonTypes=[string]}" +
                "  stringProperty: String!" +
                "}"
        );

    }

    String serialize(SpecificationTraverser traverser) {
        StringBuilder builder = new StringBuilder();
        SchemaPrinter printer = new SchemaPrinter();
        for (GraphQLType type : traverser.getGraphQLTypes()) {
            builder.append(printer.print(type));
        }
        return builder.toString();
    }
}