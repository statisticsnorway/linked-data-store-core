package no.ssb.lds.graphqlneo4j;

import org.testng.annotations.Test;

import java.util.Set;

public class CypherQueryTransformerTest {

    @Test
    public void gsim1() {
        String cypherQuery = """
                MATCH (instanceVariable:InstanceVariable)
                WHERE ALL(
                    instanceVariable_RepresentedVariable_Cond IN [
                      (instanceVariable)-[:representedVariable]->(instanceVariable_RepresentedVariable)
                      | ( ALL(
                            instanceVariable_RepresentedVariable_MultilingualText_Cond IN [
                              (instanceVariable_RepresentedVariable)-[:name]->(instanceVariable_RepresentedVariable_MultilingualText) | (instanceVariable_RepresentedVariable_MultilingualText.languageText CONTAINS 'Person')
                            ]
                            WHERE instanceVariable_RepresentedVariable_MultilingualText_Cond
                        ))
                    ]
                    WHERE instanceVariable_RepresentedVariable_Cond
                  )
                RETURN instanceVariable LIMIT 10""";
        String transformedCypher = new CypherQueryTransformer(Set.of("InstanceVariable", "RepresentedVariable")).transform(cypherQuery);
        System.out.printf("%s%n", transformedCypher);
    }

    @Test
    public void gsim2() {
        String cypherQuery = """
                MATCH (instanceVariable:InstanceVariable) WHERE ALL(instanceVariable_RepresentedVariable_Cond IN [(instanceVariable)-[:representedVariable]->(instanceVariable_RepresentedVariable) | ( ALL(instanceVariable_RepresentedVariable_MultilingualText_Cond IN [(instanceVariable_RepresentedVariable)-[:description]->(instanceVariable_RepresentedVariable_MultilingualText) | (instanceVariable_RepresentedVariable_MultilingualText.languageText CONTAINS $filterInstanceVariable_RepresentedVariable_MultilingualTextLanguageText_C)] WHERE instanceVariable_RepresentedVariable_MultilingualText_Cond))] WHERE instanceVariable_RepresentedVariable_Cond)
                RETURN instanceVariable { .id, .shortName, representedVariable:[instanceVariableRepresentedVariable IN apoc.cypher.runFirstColumnSingle('WITH $this AS this, $ver AS ver MATCH (this)-[:representedVariable]->(:RESOURCE)<-[v:VERSION_OF]-(n) WHERE v.from <= ver AND coalesce(ver < v.to, true) RETURN n', { this:instanceVariable, ver:$_version }) | instanceVariableRepresentedVariable { name:[(instanceVariableRepresentedVariable)-[:name]->(instanceVariableRepresentedVariableName:MultilingualText) | instanceVariableRepresentedVariableName { .languageText }] }][0] } AS instanceVariable LIMIT 10
                """;
        String transformedCypher = new CypherQueryTransformer(Set.of("InstanceVariable", "RepresentedVariable")).transform(cypherQuery);
        System.out.printf("%s%n", transformedCypher);
    }

}