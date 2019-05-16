package no.ssb.lds.graphql.schemas;

import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import graphql.schema.idl.TypeDefinitionRegistry;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElementType;
import no.ssb.lds.core.specification.SpecificationBuilder;
import no.ssb.lds.core.specification.TestSpecificationElement;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;

import static no.ssb.lds.core.specification.SpecificationBuilder.arrayNode;
import static no.ssb.lds.core.specification.SpecificationBuilder.booleanNode;
import static no.ssb.lds.core.specification.SpecificationBuilder.createSpecificationAndRoot;
import static no.ssb.lds.core.specification.SpecificationBuilder.numericNode;
import static no.ssb.lds.core.specification.SpecificationBuilder.objectNode;
import static no.ssb.lds.core.specification.SpecificationBuilder.required;
import static no.ssb.lds.core.specification.SpecificationBuilder.stringNode;
import static org.assertj.core.api.Assertions.assertThat;

public class SpecificationConverterTest {

    private SpecificationConverter converter;

    @BeforeMethod
    public void setUp() {
        converter = new SpecificationConverter();
    }

    private String serializeSchema(TypeDefinitionRegistry registry) {
        GraphQLSchema schema = GraphQLSchemaBuilder.parseSchema(registry);
        return new SchemaPrinter().print(schema);
    }

    @Test
    public void testRequired() {

        TestSpecificationElement embeddedObject = objectNode("EmbeddedObject", Set.of(
                required(stringNode("stringProperty")),
                required(arrayNode("stringArrayProperty", stringNode("string"))),
                required(numericNode("numericProperty")),
                required(arrayNode("numericArrayProperty", numericNode("numeric"))),
                required(booleanNode("booleanProperty")),
                required(arrayNode("booleanArrayProperty", booleanNode("boolean")))
        ));

        Specification specification = createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "Object", Set.of(
                        required(stringNode("stringProperty")),
                        required(arrayNode("stringArrayProperty", stringNode("string"))),
                        required(numericNode("numericProperty")),
                        required(arrayNode("numericArrayProperty", numericNode("numeric"))),
                        required(booleanNode("booleanProperty")),
                        required(arrayNode("booleanArrayProperty", booleanNode("boolean"))),
                        required(arrayNode("embeddedObjects", embeddedObject)),
                        required(embeddedObject)
                )),
                embeddedObject
        ));

        TypeDefinitionRegistry registry = converter.convert(specification);
        assertThat(serializeSchema(registry)).isEqualToIgnoringWhitespace("" +
                "\"Description: TestSpecificationElement{path='$.EmbeddedObject', Type=EMBEDDED, jsonTypes=[object]}\"" +
                "type EmbeddedObject {" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject.booleanArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  booleanArrayProperty: [Boolean]!" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject.booleanProperty', Type=EMBEDDED, jsonTypes=[boolean]}\"" +
                "  booleanProperty: Boolean!" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject.numericArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  numericArrayProperty: [Float]!" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject.numericProperty', Type=EMBEDDED, jsonTypes=[number]}\"" +
                "  numericProperty: Float!" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject.stringArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  stringArrayProperty: [String]!" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject.stringProperty', Type=EMBEDDED, jsonTypes=[string]}\"" +
                "  stringProperty: String!" +
                "}" +
                "" +
                "\"Description: TestSpecificationElement{path='$', Type=MANAGED, jsonTypes=[object]}\"" +
                "type Object @domain {" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject', Type=EMBEDDED, jsonTypes=[object]}\"" +
                "  EmbeddedObject: EmbeddedObject!" +
                "  \"Description: TestSpecificationElement{path='$.booleanArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  booleanArrayProperty: [Boolean]!" +
                "  \"Description: TestSpecificationElement{path='$.booleanProperty', Type=EMBEDDED, jsonTypes=[boolean]}\"" +
                "  booleanProperty: Boolean!" +
                "  \"Description: TestSpecificationElement{path='$.embeddedObjects', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  embeddedObjects: [EmbeddedObject]!" +
                "  \"Description: TestSpecificationElement{path='$.numericArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  numericArrayProperty: [Float]!" +
                "  \"Description: TestSpecificationElement{path='$.numericProperty', Type=EMBEDDED, jsonTypes=[number]}\"" +
                "  numericProperty: Float!" +
                "  \"Description: TestSpecificationElement{path='$.stringArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  stringArrayProperty: [String]!" +
                "  \"Description: TestSpecificationElement{path='$.stringProperty', Type=EMBEDDED, jsonTypes=[string]}\"" +
                "  stringProperty: String!" +
                "}" +
                "" +
                "type Query {" +
                "}"
        );
    }

    @Test
    public void testLinks() {
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

        TypeDefinitionRegistry registry = converter.convert(specification);
        assertThat(serializeSchema(registry)).isEqualToIgnoringWhitespace("" +
                "\"Description: TestSpecificationElement{path='$.EmbeddedObject', Type=EMBEDDED, jsonTypes=[object]}\"" +
                "type EmbeddedObject {" +
                "  \"Description: TestSpecificationElement{path='$.LeafObject', Type=EMBEDDED, jsonTypes=[object]}\"" +
                "  LeafObject: LeafObject" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject.stringProperty', Type=EMBEDDED, jsonTypes=[string]}\"" +
                "  stringProperty: String" +
                "}" +
                "" +
                "\"Description: TestSpecificationElement{path='$.LeafObject', Type=EMBEDDED, jsonTypes=[object]}\"" +
                "type LeafObject {" +
                "  \"Description: TestSpecificationElement{path='$.LeafObject.oneToMany', Type=REF, jsonTypes=[array]}\"" +
                "  oneToMany: [OtherObject] @link" +
                "  \"Description: TestSpecificationElement{path='$.LeafObject.oneToOne', Type=REF, jsonTypes=[string]}\"" +
                "  oneToOne: OtherObject @link" +
                "}" +
                "" +
                "\"Description: TestSpecificationElement{path='$', Type=MANAGED, jsonTypes=[object]}\"" +
                "type Object @domain {" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject', Type=EMBEDDED, jsonTypes=[object]}\"" +
                "  EmbeddedObject: EmbeddedObject" +
                "  \"Description: TestSpecificationElement{path='$.stringProperty', Type=EMBEDDED, jsonTypes=[string]}\"" +
                "  stringProperty: String" +
                "}" +
                "" +
                "\"Description: TestSpecificationElement{path='$', Type=MANAGED, jsonTypes=[object]}\"" +
                "type OtherObject @domain {" +
                "  \"Description: TestSpecificationElement{path='$.stringProperty', Type=EMBEDDED, jsonTypes=[string]}\"" +
                "  stringProperty: String" +
                "}" +
                "" +
                "type Query {" +
                "}"
        );
    }


    @Test
    public void testObject() {

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
                        arrayNode("embeddedObjects", embeddedObject),
                        embeddedObject
                )),
                embeddedObject,
                leafObject
        ));

        TypeDefinitionRegistry registry = converter.convert(specification);
        assertThat(serializeSchema(registry)).isEqualToIgnoringWhitespace("" +
                "\"Description: TestSpecificationElement{path='$.EmbeddedObject', Type=EMBEDDED, jsonTypes=[object]}\"" +
                "type EmbeddedObject {" +
                "  \"Description: TestSpecificationElement{path='$.LeafObject', Type=EMBEDDED, jsonTypes=[object]}\"" +
                "  LeafObject: LeafObject" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject.booleanArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  booleanArrayProperty: [Boolean]" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject.booleanProperty', Type=EMBEDDED, jsonTypes=[boolean]}\"" +
                "  booleanProperty: Boolean" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject.numericArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  numericArrayProperty: [Float]" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject.numericProperty', Type=EMBEDDED, jsonTypes=[number]}\"" +
                "  numericProperty: Float" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject.stringArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  stringArrayProperty: [String]" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject.stringProperty', Type=EMBEDDED, jsonTypes=[string]}\"" +
                "  stringProperty: String" +
                "}" +
                "" +
                "\"Description: TestSpecificationElement{path='$.LeafObject', Type=EMBEDDED, jsonTypes=[object]}\"" +
                "type LeafObject {" +
                "  \"Description: TestSpecificationElement{path='$.LeafObject.booleanArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  booleanArrayProperty: [Boolean]" +
                "  \"Description: TestSpecificationElement{path='$.LeafObject.booleanProperty', Type=EMBEDDED, jsonTypes=[boolean]}\"" +
                "  booleanProperty: Boolean" +
                "  \"Description: TestSpecificationElement{path='$.LeafObject.numericArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  numericArrayProperty: [Float]" +
                "  \"Description: TestSpecificationElement{path='$.LeafObject.numericProperty', Type=EMBEDDED, jsonTypes=[number]}\"" +
                "  numericProperty: Float" +
                "  \"Description: TestSpecificationElement{path='$.LeafObject.stringArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  stringArrayProperty: [String]" +
                "  \"Description: TestSpecificationElement{path='$.LeafObject.stringProperty', Type=EMBEDDED, jsonTypes=[string]}\"" +
                "  stringProperty: String" +
                "}" +
                "" +
                "\"Description: TestSpecificationElement{path='$', Type=MANAGED, jsonTypes=[object]}\"" +
                "type Object @domain {" +
                "  \"Description: TestSpecificationElement{path='$.EmbeddedObject', Type=EMBEDDED, jsonTypes=[object]}\"" +
                "  EmbeddedObject: EmbeddedObject" +
                "  \"Description: TestSpecificationElement{path='$.booleanArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  booleanArrayProperty: [Boolean]" +
                "  \"Description: TestSpecificationElement{path='$.booleanProperty', Type=EMBEDDED, jsonTypes=[boolean]}\"" +
                "  booleanProperty: Boolean" +
                "  \"Description: TestSpecificationElement{path='$.embeddedObjects', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  embeddedObjects: [EmbeddedObject]" +
                "  \"Description: TestSpecificationElement{path='$.numericArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  numericArrayProperty: [Float]" +
                "  \"Description: TestSpecificationElement{path='$.numericProperty', Type=EMBEDDED, jsonTypes=[number]}\"" +
                "  numericProperty: Float" +
                "  \"Description: TestSpecificationElement{path='$.stringArrayProperty', Type=EMBEDDED, jsonTypes=[array]}\"" +
                "  stringArrayProperty: [String]" +
                "  \"Description: TestSpecificationElement{path='$.stringProperty', Type=EMBEDDED, jsonTypes=[string]}\"" +
                "  stringProperty: String" +
                "}" +
                "" +
                "type Query {" +
                "}"
        );

    }
}