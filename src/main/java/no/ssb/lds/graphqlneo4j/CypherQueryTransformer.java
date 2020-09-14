package no.ssb.lds.graphqlneo4j;

import no.ssb.lds.cypher.CypherBaseVisitor;
import no.ssb.lds.cypher.CypherLexer;
import no.ssb.lds.cypher.CypherParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Optional.ofNullable;

public class CypherQueryTransformer {

    final Set<String> domains;

    public CypherQueryTransformer(Set<String> domains) {
        this.domains = domains;
    }

    public String transform(String query) {

        // parse query
        CypherLexer lexer = new CypherLexer(CharStreams.fromString(query, "query"));
        CypherParser parser = new CypherParser(new CommonTokenStream(lexer));
        if (parser.getNumberOfSyntaxErrors() > 0) {
            throw new IllegalArgumentException("The current ANTLR grammar does not support the given query");
        }

        // apply transformation
        String transformedQuery = new ContextVisitor().visit(parser.cypherPart());

        // re-validate that output is valid cypher - fail-fast in case of cypher syntax bugs produced by our visitor
        if (!new CypherQueryTransformer(domains).valid(transformedQuery)) {
            throw new IllegalStateException("Syntactically invalid cypher produced!");
        }

        return transformedQuery;
    }

    public boolean valid(String cypher) {
        CypherLexer lexer = new CypherLexer(CharStreams.fromString(cypher, "cypher-validation-check"));
        CypherParser parser = new CypherParser(new CommonTokenStream(lexer));
        return parser.getNumberOfSyntaxErrors() == 0;
    }

    class ContextVisitor extends CypherBaseVisitor<String> {
        int matchClauses = 0;
        int vNum = 0;
        Map<Class<?>, Function<ParserRuleContext, String>> contextFunctions = new LinkedHashMap<>();

        private <T> void setContextFunc(Class<? extends T> key, Function<ParserRuleContext, String> function) {
            contextFunctions.put(key, function);
        }

        private <T> Function<ParserRuleContext, String> removeContextFunc(Class<? extends T> key) {
            return contextFunctions.remove(key);
        }

        private <T> Function<ParserRuleContext, String> getContextFunc(Class<? extends T> key) {
            return contextFunctions.get(key);
        }

        @Override
        public String visitMatchClause(CypherParser.MatchClauseContext ctx) {
            if (matchClauses++ == 0) {
                setContextFunc(CypherParser.MatchClauseContext.class, this::modifyTopLevelMatch);
            }
            return visitChildren(ctx);
        }

        private String modifyTopLevelMatch(ParserRuleContext untypedMatchContext) {
            setContextFunc(CypherParser.PatternContext.class, this::modifyMatchPattern);
            CypherParser.MatchClauseContext mc = (CypherParser.MatchClauseContext) untypedMatchContext;
            String result = this.defaultResult();
            int n = mc.getChildCount();
            for (int i = 0; i < n && this.shouldVisitNextChild(mc, result); ++i) {
                ParseTree pt = mc.getChild(i);
                if (pt instanceof CypherParser.WhereContext) {
                    continue; // handled separately after for loop
                }
                String childResult = pt.accept(this);
                result = this.aggregateResult(result, childResult);
            }
            CypherParser.WhereContext whereContext = mc.where();
            if (whereContext != null) {
                result += whereContext.WHERE();
                result += whereContext.SP();
                result += "(_v.from <= $_version AND coalesce($_version < _v.to, true))\n  AND (";
                setContextFunc(CypherParser.FilterExpressionContext.class, this::modifyFilterExpression);
                result += whereContext.expression().accept(this);
                result += ") ";
            } else {
                result += " WHERE (_v.from <= $_version AND coalesce($_version < _v.to, true)) ";
            }
            return result;
        }

        private String modifyMatchPattern(ParserRuleContext untypedPatternContext) {
            String result = untypedPatternContext.accept(this);
            Matcher m = Pattern.compile("\\(([^:]*):([^)]*)\\)").matcher(result);
            if (!m.matches()) {
                throw new IllegalArgumentException("CYPHER: " + result + "\ndoes not match pattern.");
            }
            String nodeIdentifierLiteral = m.group(1);
            String nodeTypeLiteral = m.group(2);
            String modifiedResult = String.format("(_r:%s_R)<-[_v:VERSION_OF]-(%s)", nodeTypeLiteral, nodeIdentifierLiteral);
            return modifiedResult;
        }

        private String modifyFilterExpression(ParserRuleContext untypedContext) {
            CypherParser.FilterExpressionContext filterExpressionContext = (CypherParser.FilterExpressionContext) untypedContext;
            String variableText = filterExpressionContext.idInColl().variable().getText();
            String whereExpressionText = filterExpressionContext.where().expression().getText();
            if (variableText.endsWith("_Cond") && variableText.equals(whereExpressionText)) {
                setContextFunc(CypherParser.PatternComprehensionContext.class, this::modifyPatternComprehension);
            }
            return filterExpressionContext.accept(this);
        }

        private String modifyPatternComprehension(ParserRuleContext parserRuleContext) {
            setContextFunc(CypherParser.RelationshipsPatternContext.class, this::modifyRelationshipsPattern);
            setContextFunc(CypherParser.FilterExpressionContext.class, this::modifyFilterExpression);
            return parserRuleContext.accept(this);
        }

        private String modifyRelationshipsPattern(ParserRuleContext parserRuleContext) {
            CypherParser.RelationshipsPatternContext relationshipsPatternContext = (CypherParser.RelationshipsPatternContext) parserRuleContext;
            String relationshipsPatternText = relationshipsPatternContext.getText();
            Matcher m = Pattern.compile("\\((?<sourceNode>[^)]+)\\)-\\[:(?<sourceRelation>[^]]+)\\]->\\((?<targetNode>[^)]+)\\)").matcher(relationshipsPatternText);
            if (!m.matches()) {
                return relationshipsPatternText;
            }
            String sourceNode = m.group("sourceNode");
            String sourceRelation = m.group("sourceRelation");
            String targetNode = m.group("targetNode");
            String targetType = targetNode.substring((sourceNode + "_").length());
            if (!domains.contains(targetType)) {
                return relationshipsPatternText; // relation to embedded type
            }
            StringBuilder sb = new StringBuilder();
            sb.append("(").append(sourceNode).append(")");
            sb.append("-[:").append(sourceRelation).append("]->");
            sb.append("(:").append(targetType + "_R").append(")");
            String versionRelationIdentifier = "_v" + vNum++;
            sb.append("<-[").append(versionRelationIdentifier).append(":VERSION_OF]-");
            sb.append("(").append(targetNode).append(")");
            sb.append(" WHERE (").append(versionRelationIdentifier).append(".from <= $_version AND coalesce($_version < ").append(versionRelationIdentifier).append(".to, true))");
            return sb.toString();
        }

        @Override
        public String visitChildren(RuleNode node) {
            Function<ParserRuleContext, String> function = removeContextFunc(node.getClass());
            Map<Class<?>, Function<ParserRuleContext, String>> parentContextFunctions = contextFunctions;
            contextFunctions = new LinkedHashMap<>(contextFunctions);
            try {
                if (function == null) {
                    return super.visitChildren(node);
                }
                return function.apply((ParserRuleContext) node);
            } finally {
                contextFunctions = parentContextFunctions;
            }
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
