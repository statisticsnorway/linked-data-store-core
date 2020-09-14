package no.ssb.lds.graphqlneo4j;

import no.ssb.lds.cypher.CypherBaseVisitor;
import no.ssb.lds.cypher.CypherLexer;
import no.ssb.lds.cypher.CypherParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.Set;

import static java.util.Optional.ofNullable;

public class CypherQueryProjectionSplitter {

    final Set<String> domains;

    String selection;
    String projection;

    public CypherQueryProjectionSplitter(Set<String> domains) {
        this.domains = domains;
    }

    public String getSelection() {
        return selection;
    }

    public String getProjection() {
        return projection;
    }

    public String transform(String query) {

        // parse query
        CypherLexer lexer = new CypherLexer(CharStreams.fromString(query, "query"));
        CypherParser parser = new CypherParser(new CommonTokenStream(lexer));
        if (parser.getNumberOfSyntaxErrors() > 0) {
            throw new IllegalArgumentException("The current ANTLR grammar does not support the given query");
        }

        // apply transformation
        selection = new ContextVisitor().visit(parser.cypherPart());

        // re-validate that output is valid cypher - fail-fast in case of cypher syntax bugs produced by our visitor
        if (!new CypherQueryProjectionSplitter(domains).valid(selection)) {
            throw new IllegalStateException("Syntactically invalid cypher produced!");
        }

        return selection;
    }

    public boolean valid(String cypher) {
        CypherLexer lexer = new CypherLexer(CharStreams.fromString(cypher, "cypher-validation-check"));
        CypherParser parser = new CypherParser(new CommonTokenStream(lexer));
        return parser.getNumberOfSyntaxErrors() == 0;
    }

    class ContextVisitor extends CypherBaseVisitor<String> {

        boolean doMatch;
        boolean doPatternElementChain;
        boolean doNodePattern;
        boolean doVariable;
        boolean doLabelName;
        String topLevelDomainTypeName = "##Unresolved##";
        String topLevelDomainIdentifier = "##Unresolved##";

        @Override
        public String visitSingleQuery(CypherParser.SingleQueryContext node) {
            doMatch = true;

            String result = this.defaultResult();
            int n = node.getChildCount();

            for (int i = 0; i < (n - 1) && this.shouldVisitNextChild(node, result); ++i) {
                ParseTree c = node.getChild(i);
                String childResult = c.accept(this);
                result = this.aggregateResult(result, childResult);
            }

            List<CypherParser.ClauseContext> clauses = node.clause();
            CypherParser.ClauseContext lastChildClauseOfSingleQuery = clauses.get(clauses.size() - 1);
            String returnClauseText = lastChildClauseOfSingleQuery.accept(this);

            StringBuilder sb = new StringBuilder();
            sb.append("MATCH (").append(topLevelDomainIdentifier).append(":").append(topLevelDomainTypeName).append(") ");
            sb.append("WHERE id(").append(topLevelDomainIdentifier).append(") IN $ids ");
            sb.append(returnClauseText);
            projection = sb.toString();

            String limit = ofNullable(lastChildClauseOfSingleQuery.returnClause())
                    .map(CypherParser.ReturnClauseContext::returnBody)
                    .map(CypherParser.ReturnBodyContext::limit)
                    .map(RuleContext::getText)
                    .orElse("");

            return result + "RETURN id(" + topLevelDomainIdentifier + ") AS id " + limit;
        }

        @Override
        public String visitMatchClause(CypherParser.MatchClauseContext ctx) {
            if (doMatch) {
                doMatch = false;
                doLabelName = true;
                doPatternElementChain = true;
            }
            return super.visitMatchClause(ctx);
        }

        @Override
        public String visitPatternElementChain(CypherParser.PatternElementChainContext ctx) {
            if (doPatternElementChain) {
                doPatternElementChain = false;
                doNodePattern = true;
            }
            return super.visitPatternElementChain(ctx);
        }

        @Override
        public String visitNodePattern(CypherParser.NodePatternContext ctx) {
            if (doNodePattern) {
                doNodePattern = false;
                doVariable = true;
            }
            return super.visitNodePattern(ctx);
        }

        @Override
        public String visitVariable(CypherParser.VariableContext ctx) {
            if (doVariable) {
                doVariable = false;
                topLevelDomainIdentifier = ctx.getText();
            }
            return super.visitVariable(ctx);
        }

        @Override
        public String visitLabelName(CypherParser.LabelNameContext ctx) {
            if (doLabelName) {
                doLabelName = false;
                topLevelDomainTypeName = ctx.getText();
                topLevelDomainTypeName = topLevelDomainTypeName.substring(0, topLevelDomainTypeName.length() - "_R".length());
            }
            return super.visitLabelName(ctx);
        }

        @Override
        public String visitTerminal(TerminalNode node) {
            return node.getText();
        }

        @Override
        protected String aggregateResult(String aggregate, String nextResult) {
            return ofNullable(aggregate).orElse("") + ofNullable(nextResult).orElse("");
        }
    }
}
