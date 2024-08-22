package com.example.spring.netty.spring_netty.utils.sql;

import com.example.spring.netty.spring_netty.utils.sql.semantic.PairConversion;
import com.example.spring.netty.spring_netty.utils.sql.semantic.SemanticNamesConversion;
import com.example.spring.netty.spring_netty.utils.sql.semantic.SemanticTableConversion;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.AllValue;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.ConnectByRootOperator;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonAggregateFunction;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.JsonFunction;
import net.sf.jsqlparser.expression.JsonFunctionExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.OracleNamedFunctionParameter;
import net.sf.jsqlparser.expression.OverlapsCondition;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RangeExpression;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.RowGetExpression;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.TimezoneExpression;
import net.sf.jsqlparser.expression.TranscodingFunction;
import net.sf.jsqlparser.expression.TrimFunction;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.VariableAssignment;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.XMLSerializeExpr;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.IntegerDivision;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ContainedBy;
import net.sf.jsqlparser.expression.operators.relational.Contains;
import net.sf.jsqlparser.expression.operators.relational.DoubleAnd;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.FullTextSearch;
import net.sf.jsqlparser.expression.operators.relational.GeometryDistance;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MemberOfExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.expression.operators.relational.TSQLLeftJoin;
import net.sf.jsqlparser.expression.operators.relational.TSQLRightJoin;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Block;
import net.sf.jsqlparser.statement.Commit;
import net.sf.jsqlparser.statement.CreateFunctionalStatement;
import net.sf.jsqlparser.statement.DeclareStatement;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.IfElseStatement;
import net.sf.jsqlparser.statement.PurgeObjectType;
import net.sf.jsqlparser.statement.PurgeStatement;
import net.sf.jsqlparser.statement.ResetStatement;
import net.sf.jsqlparser.statement.RollbackStatement;
import net.sf.jsqlparser.statement.SavepointStatement;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.ShowColumnsStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.UnsupportedStatement;
import net.sf.jsqlparser.statement.UseStatement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterSession;
import net.sf.jsqlparser.statement.alter.AlterSystemStatement;
import net.sf.jsqlparser.statement.alter.RenameTableStatement;
import net.sf.jsqlparser.statement.alter.sequence.AlterSequence;
import net.sf.jsqlparser.statement.analyze.Analyze;
import net.sf.jsqlparser.statement.comment.Comment;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.schema.CreateSchema;
import net.sf.jsqlparser.statement.create.sequence.CreateSequence;
import net.sf.jsqlparser.statement.create.synonym.CreateSynonym;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.grant.Grant;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.refresh.RefreshMaterializedViewStatement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.GroupByVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.TableStatement;
import net.sf.jsqlparser.statement.select.Values;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.show.ShowIndexStatement;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Getter
@NoArgsConstructor
@SuppressWarnings({"PMD.CyclomaticComplexity", "PMD.UncommentedEmptyMethodBody"})
public class SemanticConvertor implements SelectVisitor, FromItemVisitor, ExpressionVisitor,
        SelectItemVisitor, StatementVisitor, GroupByVisitor, OrderByVisitor {

    private static final String NOT_SUPPORTED_YET = "Not supported yet.";
    private Statement statement;
    private SemanticNamesConversion semanticNamesConversion;
    private final String EMPTY = "";
    private String tableContext = EMPTY;
    private String dictGetContext = EMPTY;

    @SneakyThrows
    public SemanticConvertor(String sqlStr, SemanticNamesConversion semanticNamesConversion) {
        this.statement = CCJSqlParserUtil.parse(sqlStr);
        this.semanticNamesConversion = semanticNamesConversion;
    }

    public void setTableContext(String tableName) {
        tableContext = tableName;
        log.info("Set semantic table context to '{}'", tableName);
    }

    public void unsetTableContext() {
        if (!EMPTY.equalsIgnoreCase(tableContext)) {
            log.info("Unset semantic table context for '{}'", tableContext);
            tableContext = EMPTY;
        }
    }

    public void setDictGetContext(String dictGetContext) {
        this.dictGetContext = dictGetContext;
        log.info("Set semantic dictGet context to '{}'", dictGetContext);
    }

    public void unsetDictGetContext() {
        if (!EMPTY.equalsIgnoreCase(dictGetContext)) {
            log.info("Unset semantic dictGet context for '{}'", dictGetContext);
            dictGetContext = EMPTY;
        }
    }

    public void printStatement() {
        log.info("Statement: {}", statement);
    }

    public Statement rewrite(Statement statement) {
        statement.accept(this);
        log.info("Rewritten statement: {}", statement);
        return statement;
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        if (plainSelect.getFromItem() != null
                && (semanticNamesConversion.getSemanticTableNameIfAny(plainSelect.getFromItem().toString()).isPresent()
                    || (plainSelect.getFromItem().getAlias() != null
                        && semanticNamesConversion.getSemanticTableNameIfAny(plainSelect.getFromItem().toString().split(plainSelect.getFromItem().getAlias().toString())[0].trim()).isPresent())
        )) {
            if (plainSelect.getFromItem().getAlias() != null) {
                setTableContext(plainSelect.getFromItem().toString().split(plainSelect.getFromItem().getAlias().toString())[0].trim());
            } else {
                setTableContext(plainSelect.getFromItem().toString());
            }
        }

        List<WithItem> withItemsList = plainSelect.getWithItemsList();
        if (withItemsList != null && !withItemsList.isEmpty()) {
            for (WithItem withItem : withItemsList) {
                withItem.accept((SelectVisitor) this);
            }
        }
        if (plainSelect.getSelectItems() != null) {
            for (SelectItem<?> item : plainSelect.getSelectItems()) {
                item.accept(this);
            }
        }

        if (plainSelect.getFromItem() != null) {
            plainSelect.getFromItem().accept(this);
        }

        visitJoins(plainSelect.getJoins());
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(this);
        }

        if (plainSelect.getHaving() != null) {
            plainSelect.getHaving().accept(this);
        }

        if (plainSelect.getOracleHierarchical() != null) {
            plainSelect.getOracleHierarchical().accept(this);
        }

        if (plainSelect.getGroupBy() != null) {
            plainSelect.getGroupBy().accept(this);
        }

        if (plainSelect.getOrderByElements() != null) {
            plainSelect.getOrderByElements().forEach(e -> e.accept(this));
        }

        unsetTableContext();
    }

    @Override
    public void visit(Function function) {
        if ("dictGet".equalsIgnoreCase(function.getName())) {
            String tableName = function.getParameters().get(0).toString();
            Optional<SemanticTableConversion> conversion = semanticNamesConversion.getSemanticTableNameIfAny(tableName);
            if (conversion.isPresent()) {
                setDictGetContext(tableName);
            }
        }
        ExpressionList exprList = function.getParameters();
        if (exprList != null) {
            visit(exprList);
        }
        if ("dictGet".equalsIgnoreCase(function.getName())) {
            unsetDictGetContext();
        }
    }

    @Override
    public void visit(GroupByElement groupBy) {
        groupBy.getGroupByExpressionList().accept(this);
    }

    @Override
    public void visit(OrderByElement orderBy) {
        orderBy.getExpression().accept(this);
    }

    @Override
    public void visit(Table tableName) {
        if (!EMPTY.equals(tableContext)) {
            Optional<SemanticTableConversion> conversion = semanticNamesConversion.getSemanticTableNameIfAny(tableContext);
            if (conversion.isPresent()) {
                tableName.withName(conversion.get().getTableConversion().realName());
                if (tableContext.contains(".")) {
                    tableName.setSchemaName(conversion.get().getSchemaConversion().realName());
                }
                log.info("Rewrite table with semantic name '{}' to real name '{}' in table context '{}'", conversion.get().getTableConversion().semanticName(), conversion.get().getTableConversion().realName(), tableContext);
            }
        }
    }

    @Override
    public void visit(Column tableColumn) {
        if (!EMPTY.equals(tableContext)) {
            Optional<SemanticTableConversion> conversion = semanticNamesConversion.getSemanticTableNameIfAny(tableContext);
            if (conversion.isPresent() && conversion.get().getColumnConversions() != null) {
                AtomicReference<String> name = new AtomicReference<>(tableColumn.getFullyQualifiedName());
                Optional<PairConversion<String, String>> columnConversion = conversion.get().findColumnConversionBySemanticName(name.get());
                if (columnConversion.isPresent()) {
                    tableColumn.withColumnName(columnConversion.get().realName());
                    log.info("Rewrite column with semantic name '{}' to real name '{}' in table context '{}'", tableColumn.getFullyQualifiedName(), columnConversion.get().realName(), tableContext);
                }
            }
        }
        if (tableColumn.getTable() != null
                && tableColumn.getTable().getName() != null) {
            visit(tableColumn.getTable());
        }
    }

    @Override
    public void visit(SelectItem item) {
        boolean changed = false;
        if (!EMPTY.equals(tableContext)) {
            Optional<SemanticTableConversion> conversion = semanticNamesConversion.getSemanticTableNameIfAny(tableContext);
            if (conversion.isPresent() && conversion.get().getColumnConversions() != null) {
                if (item.getAlias() == null) {
                    AtomicReference<String> name = new AtomicReference<>(item.getExpression().toString());
                    Optional<PairConversion<String, String>> columnConversion = conversion.get().findColumnConversionBySemanticName(name.get());
                    if (columnConversion.isPresent()) {
                        item.withAlias(new Alias(item.toString()));
                        changed = true;
                    }
                }
            }
        }
        item.getExpression().accept(this);
        if (changed) {
            log.info("Set alias '{}' for SelectItem in table context '{}'", item, tableContext);
        }
    }

    @Override
    public void visit(Select select) {
        List<WithItem> withItemsList = select.getWithItemsList();
        if (withItemsList != null && !withItemsList.isEmpty()) {
            for (WithItem withItem : withItemsList) {
                withItem.accept((SelectVisitor) this);
            }
        }
        select.accept((SelectVisitor) this);
    }

    @Override
    public void visit(StringValue stringValue) {
        if (!EMPTY.equalsIgnoreCase(getDictGetContext())) {
            Optional<SemanticTableConversion> conversion = semanticNamesConversion.getSemanticTableNameIfAny(getDictGetContext());
            if (conversion.isPresent()) {
                /* need to identify is it a column or table */
                Optional<SemanticTableConversion> conv = semanticNamesConversion.getSemanticTableNameIfAny(stringValue.getValue());
                if (conv.isPresent()) {
                    String newValue = String.format("%s.%s", conv.get().getSchemaConversion().realName(), conv.get().getTableConversion().realName());
                    log.info("Rewrite string value with semantic name '{}' to real name '{}' in dictGet context '{}'", stringValue.getValue(), newValue, dictGetContext);
                    stringValue.setValue(newValue);
                    return;
                }
                Optional<PairConversion<String, String>> columnConversion = conversion.get().findColumnConversionBySemanticName(stringValue.getValue());
                if (columnConversion.isPresent()) {
                    log.info("Rewrite string value with semantic name '{}' to real name '{}' in dictGet context '{}'", stringValue.getValue(), columnConversion.get().realName(), dictGetContext);
                    stringValue.setValue(columnConversion.get().realName());
                }
            }
        }
    }

    @Override
    public void visit(TranscodingFunction transcodingFunction) {
        transcodingFunction.getExpression().accept(this);
    }

    @Override
    public void visit(TrimFunction trimFunction) {
        if (trimFunction.getExpression() != null) {
            trimFunction.getExpression().accept(this);
        }
        if (trimFunction.getFromExpression() != null) {
            trimFunction.getFromExpression().accept(this);
        }
    }

    @Override
    public void visit(RangeExpression rangeExpression) {
        rangeExpression.getStartExpression().accept(this);
        rangeExpression.getEndExpression().accept(this);
    }

    @Override
    public void visit(WithItem withItem) {
        withItem.getSelect().accept((SelectVisitor) this);
    }

    @Override
    public void visit(ParenthesedSelect selectBody) {
        List<WithItem> withItemsList = selectBody.getWithItemsList();
        if (withItemsList != null && !withItemsList.isEmpty()) {
            for (WithItem withItem : withItemsList) {
                withItem.accept((SelectVisitor) this);
            }
        }
        selectBody.getSelect().accept((SelectVisitor) this);
    }

    @Override
    public void visit(Addition addition) {
        visitBinaryExpression(addition);
    }

    @Override
    public void visit(AndExpression andExpression) {
        visitBinaryExpression(andExpression);
    }

    @Override
    public void visit(Between between) {
        between.getLeftExpression().accept(this);
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
    }

    @Override
    public void visit(OverlapsCondition overlapsCondition) {
        overlapsCondition.getLeft().accept(this);
        overlapsCondition.getRight().accept(this);
    }

    @Override
    public void visit(Division division) {
        visitBinaryExpression(division);
    }

    @Override
    public void visit(IntegerDivision division) {
        visitBinaryExpression(division);
    }

    @Override
    public void visit(DoubleValue doubleValue) {

    }

    @Override
    public void visit(EqualsTo equalsTo) {
        visitBinaryExpression(equalsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        visitBinaryExpression(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        visitBinaryExpression(greaterThanEquals);
    }

    @Override
    public void visit(InExpression inExpression) {
        inExpression.getLeftExpression().accept(this);
        inExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {

    }

    @Override
    public void visit(SignedExpression signedExpression) {
        signedExpression.getExpression().accept(this);
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {

    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {

    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {

    }

    @Override
    public void visit(LikeExpression likeExpression) {
        visitBinaryExpression(likeExpression);
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        existsExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(MemberOfExpression memberOfExpression) {
        memberOfExpression.getLeftExpression().accept(this);
        memberOfExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(LongValue longValue) {

    }

    @Override
    public void visit(MinorThan minorThan) {
        visitBinaryExpression(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        visitBinaryExpression(minorThanEquals);
    }

    @Override
    public void visit(Multiplication multiplication) {
        visitBinaryExpression(multiplication);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        visitBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(DoubleAnd doubleAnd) {
        visitBinaryExpression(doubleAnd);
    }

    @Override
    public void visit(Contains contains) {
        visitBinaryExpression(contains);
    }

    @Override
    public void visit(ContainedBy containedBy) {
        visitBinaryExpression(containedBy);
    }

    @Override
    public void visit(NullValue nullValue) {

    }

    @Override
    public void visit(OrExpression orExpression) {
        visitBinaryExpression(orExpression);
    }

    @Override
    public void visit(XorExpression xorExpression) {
        visitBinaryExpression(xorExpression);
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(Subtraction subtraction) {
        visitBinaryExpression(subtraction);
    }

    @Override
    public void visit(NotExpression notExpr) {
        notExpr.getExpression().accept(this);
    }

    @Override
    public void visit(BitwiseRightShift expr) {
        visitBinaryExpression(expr);
    }

    @Override
    public void visit(BitwiseLeftShift expr) {
        visitBinaryExpression(expr);
    }

    public void visitBinaryExpression(BinaryExpression binaryExpression) {
        binaryExpression.getLeftExpression().accept(this);
        binaryExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(ExpressionList<?> expressionList) {
        for (Expression expression : expressionList) {
            expression.accept(this);
        }
    }

    @Override
    public void visit(DateValue dateValue) {

    }

    @Override
    public void visit(TimestampValue timestampValue) {

    }

    @Override
    public void visit(TimeValue timeValue) {

    }

    /*
     * (non-Javadoc)
     *
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.
     * CaseExpression)
     */
    @Override
    public void visit(CaseExpression caseExpression) {
        if (caseExpression.getSwitchExpression() != null) {
            caseExpression.getSwitchExpression().accept(this);
        }
        if (caseExpression.getWhenClauses() != null) {
            for (WhenClause when : caseExpression.getWhenClauses()) {
                when.accept(this);
            }
        }
        if (caseExpression.getElseExpression() != null) {
            caseExpression.getElseExpression().accept(this);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.WhenClause)
     */
    @Override
    public void visit(WhenClause whenClause) {
        if (whenClause.getWhenExpression() != null) {
            whenClause.getWhenExpression().accept(this);
        }
        if (whenClause.getThenExpression() != null) {
            whenClause.getThenExpression().accept(this);
        }
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        anyComparisonExpression.getSelect().accept((ExpressionVisitor) this);
    }

    @Override
    public void visit(Concat concat) {
        visitBinaryExpression(concat);
    }

    @Override
    public void visit(Matches matches) {
        visitBinaryExpression(matches);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        visitBinaryExpression(bitwiseAnd);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        visitBinaryExpression(bitwiseOr);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        visitBinaryExpression(bitwiseXor);
    }

    @Override
    public void visit(CastExpression cast) {
        cast.getLeftExpression().accept(this);
    }

    @Override
    public void visit(Modulo modulo) {
        visitBinaryExpression(modulo);
    }

    @Override
    public void visit(AnalyticExpression analytic) {
        if (analytic.getExpression() != null) {
            analytic.getExpression().accept(this);
        }
        if (analytic.getDefaultValue() != null) {
            analytic.getDefaultValue().accept(this);
        }
        if (analytic.getOffset() != null) {
            analytic.getOffset().accept(this);
        }
        if (analytic.getKeep() != null) {
            analytic.getKeep().accept(this);
        }
        if (analytic.getFuncOrderBy() != null) {
            for (OrderByElement element : analytic.getOrderByElements()) {
                element.getExpression().accept(this);
            }
        }

        if (analytic.getWindowElement() != null) {
            analytic.getWindowElement().getRange().getStart().getExpression().accept(this);
            analytic.getWindowElement().getRange().getEnd().getExpression().accept(this);
            analytic.getWindowElement().getOffset().getExpression().accept(this);
        }
    }

    @Override
    public void visit(SetOperationList list) {
        List<WithItem> withItemsList = list.getWithItemsList();
        if (withItemsList != null && !withItemsList.isEmpty()) {
            for (WithItem withItem : withItemsList) {
                withItem.accept((SelectVisitor) this);
            }
        }
        for (Select selectBody : list.getSelects()) {
            selectBody.accept((SelectVisitor) this);
        }
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        if (eexpr.getExpression() != null) {
            eexpr.getExpression().accept(this);
        }
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        lateralSubSelect.getSelect().accept((SelectVisitor) this);
    }

    @Override
    public void visit(TableStatement tableStatement) {
        tableStatement.getTable().accept(this);
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        if (iexpr.getExpression() != null) {
            iexpr.getExpression().accept(this);
        }
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {

    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        if (oexpr.getStartExpression() != null) {
            oexpr.getStartExpression().accept(this);
        }

        if (oexpr.getConnectExpression() != null) {
            oexpr.getConnectExpression().accept(this);
        }
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        visitBinaryExpression(rexpr);
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        if (jsonExpr.getExpression() != null) {
            jsonExpr.getExpression().accept(this);
        }
    }

    @Override
    public void visit(JsonOperator jsonExpr) {
        visitBinaryExpression(jsonExpr);
    }

    @Override
    public void visit(AllColumns allColumns) {

    }

    @Override
    public void visit(AllTableColumns allTableColumns) {

    }

    @Override
    public void visit(AllValue allValue) {

    }

    @Override
    public void visit(IsDistinctExpression isDistinctExpression) {
        visitBinaryExpression(isDistinctExpression);
    }

    @Override
    public void visit(UserVariable var) {

    }

    @Override
    public void visit(NumericBind bind) {


    }

    @Override
    public void visit(KeepExpression aexpr) {

    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {

    }

    @Override
    public void visit(Delete delete) {
        visit(delete.getTable());

        if (delete.getUsingList() != null) {
            for (Table using : delete.getUsingList()) {
                visit(using);
            }
        }

        visitJoins(delete.getJoins());

        if (delete.getWhere() != null) {
            delete.getWhere().accept(this);
        }
    }

    @Override
    public void visit(Update update) {
        visit(update.getTable());
        if (update.getWithItemsList() != null) {
            for (WithItem withItem : update.getWithItemsList()) {
                withItem.accept((SelectVisitor) this);
            }
        }

        if (update.getStartJoins() != null) {
            for (Join join : update.getStartJoins()) {
                join.getRightItem().accept(this);
            }
        }
        if (update.getExpressions() != null) {
            for (Expression expression : update.getExpressions()) {
                expression.accept(this);
            }
        }

        if (update.getFromItem() != null) {
            update.getFromItem().accept(this);
        }

        if (update.getJoins() != null) {
            for (Join join : update.getJoins()) {
                join.getRightItem().accept(this);
                for (Expression expression : join.getOnExpressions()) {
                    expression.accept(this);
                }
            }
        }

        if (update.getWhere() != null) {
            update.getWhere().accept(this);
        }
    }

    @Override
    public void visit(Insert insert) {
        visit(insert.getTable());
        if (insert.getWithItemsList() != null) {
            for (WithItem withItem : insert.getWithItemsList()) {
                withItem.accept((SelectVisitor) this);
            }
        }
        if (insert.getSelect() != null) {
            visit(insert.getSelect());
        }
    }

    public void visit(Analyze analyze) {
        visit(analyze.getTable());
    }

    @Override
    public void visit(Drop drop) {
        visit(drop.getName());
    }

    @Override
    public void visit(Truncate truncate) {
        visit(truncate.getTable());
    }

    @Override
    public void visit(CreateIndex createIndex) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @Override
    public void visit(CreateSchema aThis) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @Override
    public void visit(CreateTable create) {
        visit(create.getTable());
        if (create.getSelect() != null) {
            create.getSelect().accept((SelectVisitor) this);
        }
    }

    @Override
    public void visit(CreateView createView) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @Override
    public void visit(Alter alter) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @Override
    public void visit(Statements stmts) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @Override
    public void visit(Execute execute) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @Override
    public void visit(SetStatement set) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @Override
    public void visit(ResetStatement reset) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @Override
    public void visit(ShowColumnsStatement set) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @Override
    public void visit(ShowIndexStatement showIndex) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @Override
    public void visit(RowConstructor<?> rowConstructor) {
        for (Expression expr : rowConstructor) {
            expr.accept(this);
        }
    }

    @Override
    public void visit(RowGetExpression rowGetExpression) {
        rowGetExpression.getExpression().accept(this);
    }

    @Override
    public void visit(HexValue hexValue) {


    }

    @Override
    public void visit(Merge merge) {
        visit(merge.getTable());
        if (merge.getWithItemsList() != null) {
            for (WithItem withItem : merge.getWithItemsList()) {
                withItem.accept((SelectVisitor) this);
            }
        }

        if (merge.getFromItem() != null) {
            merge.getFromItem().accept(this);
        }
    }

    @Override
    public void visit(OracleHint hint) {

    }

    @Override
    public void visit(TableFunction tableFunction) {
        visit(tableFunction.getFunction());
    }

    @Override
    public void visit(AlterView alterView) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @Override
    public void visit(RefreshMaterializedViewStatement materializedView) {
        visit(materializedView.getView());
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {

    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {


    }

    @Override
    public void visit(Commit commit) {


    }

    @Override
    public void visit(Upsert upsert) {
        visit(upsert.getTable());
        if (upsert.getExpressions() != null) {
            upsert.getExpressions().accept(this);
        }
        if (upsert.getSelect() != null) {
            visit(upsert.getSelect());
        }
    }

    @Override
    public void visit(UseStatement use) {

    }

    @Override
    public void visit(ParenthesedFromItem parenthesis) {
        parenthesis.getFromItem().accept(this);
        // support join keyword in fromItem
        visitJoins(parenthesis.getJoins());
    }

    /**
     * visit join block
     *
     * @param parenthesis join sql block
     */
    private void visitJoins(List<Join> parenthesis) {
        if (parenthesis == null) {
            return;
        }
        for (Join join : parenthesis) {
            join.getFromItem().accept(this);
            join.getRightItem().accept(this);
            for (Expression expression : join.getOnExpressions()) {
                expression.accept(this);
            }
        }
    }

    @Override
    public void visit(Block block) {
        if (block.getStatements() != null) {
            visit(block.getStatements());
        }
    }

    @Override
    public void visit(Comment comment) {
        if (comment.getTable() != null) {
            visit(comment.getTable());
        }
        if (comment.getColumn() != null) {
            Table table = comment.getColumn().getTable();
            if (table != null) {
                visit(table);
            }
        }
    }

    @Override
    public void visit(Values values) {
        values.getExpressions().accept(this);
    }

    @Override
    public void visit(DescribeStatement describe) {
        describe.getTable().accept(this);
    }

    @Override
    public void visit(ExplainStatement explain) {
        if (explain.getStatement() != null) {
            explain.getStatement().accept((StatementVisitor) this);
        }
    }

    @Override
    public void visit(NextValExpression nextVal) {

    }

    @Override
    public void visit(CollateExpression col) {
        col.getLeftExpression().accept(this);
    }

    @Override
    public void visit(ShowStatement aThis) {

    }

    @Override
    public void visit(SimilarToExpression expr) {
        visitBinaryExpression(expr);
    }

    @Override
    public void visit(DeclareStatement aThis) {

    }

    @Override
    public void visit(Grant grant) {


    }

    @Override
    public void visit(ArrayExpression array) {
        array.getObjExpression().accept(this);
        if (array.getStartIndexExpression() != null) {
            array.getIndexExpression().accept(this);
        }
        if (array.getStartIndexExpression() != null) {
            array.getStartIndexExpression().accept(this);
        }
        if (array.getStopIndexExpression() != null) {
            array.getStopIndexExpression().accept(this);
        }
    }

    @Override
    public void visit(ArrayConstructor array) {
        for (Expression expression : array.getExpressions()) {
            expression.accept(this);
        }
    }

    @Override
    public void visit(CreateSequence createSequence) {
        throw new UnsupportedOperationException(
                "Finding tables from CreateSequence is not supported");
    }

    @Override
    public void visit(AlterSequence alterSequence) {
        throw new UnsupportedOperationException(
                "Finding tables from AlterSequence is not supported");
    }

    @Override
    public void visit(CreateFunctionalStatement createFunctionalStatement) {
        throw new UnsupportedOperationException(
                "Finding tables from CreateFunctionalStatement is not supported");
    }

    @Override
    public void visit(ShowTablesStatement showTables) {
        throw new UnsupportedOperationException(
                "Finding tables from ShowTablesStatement is not supported");
    }

    @Override
    public void visit(TSQLLeftJoin tsqlLeftJoin) {
        visitBinaryExpression(tsqlLeftJoin);
    }

    @Override
    public void visit(TSQLRightJoin tsqlRightJoin) {
        visitBinaryExpression(tsqlRightJoin);
    }

    @Override
    public void visit(VariableAssignment var) {
        var.getVariable().accept(this);
        var.getExpression().accept(this);
    }

    @Override
    public void visit(XMLSerializeExpr aThis) {

    }

    @Override
    public void visit(CreateSynonym createSynonym) {
        throwUnsupported(createSynonym);
    }

    private static <T> void throwUnsupported(T type) {
        throw new UnsupportedOperationException(String.format(
                "Finding tables from %s is not supported", type.getClass().getSimpleName()));
    }

    @Override
    public void visit(TimezoneExpression aThis) {
        aThis.getLeftExpression().accept(this);
    }

    @Override
    public void visit(SavepointStatement savepointStatement) {}

    @Override
    public void visit(RollbackStatement rollbackStatement) {

    }

    @Override
    public void visit(AlterSession alterSession) {

    }

    @Override
    public void visit(JsonAggregateFunction expression) {
        Expression expr = expression.getExpression();
        if (expr != null) {
            expr.accept(this);
        }

        expr = expression.getFilterExpression();
        if (expr != null) {
            expr.accept(this);
        }
    }

    @Override
    public void visit(JsonFunction expression) {
        for (JsonFunctionExpression expr : expression.getExpressions()) {
            expr.getExpression().accept(this);
        }
    }

    @Override
    public void visit(ConnectByRootOperator connectByRootOperator) {
        connectByRootOperator.getColumn().accept(this);
    }

    public void visit(IfElseStatement ifElseStatement) {
        ifElseStatement.getIfStatement().accept(this);
        if (ifElseStatement.getElseStatement() != null) {
            ifElseStatement.getElseStatement().accept(this);
        }
    }

    public void visit(OracleNamedFunctionParameter oracleNamedFunctionParameter) {
        oracleNamedFunctionParameter.getExpression().accept(this);
    }

    @Override
    public void visit(RenameTableStatement renameTableStatement) {
        for (Map.Entry<Table, Table> e : renameTableStatement.getTableNames()) {
            e.getKey().accept(this);
            e.getValue().accept(this);
        }
    }

    @Override
    public void visit(PurgeStatement purgeStatement) {
        if (purgeStatement.getPurgeObjectType() == PurgeObjectType.TABLE) {
            ((Table) purgeStatement.getObject()).accept(this);
        }
    }

    @Override
    public void visit(AlterSystemStatement alterSystemStatement) {
        // no tables involved in this statement
    }

    @Override
    public void visit(UnsupportedStatement unsupportedStatement) {
        // no tables involved in this statement
    }

    @Override
    public void visit(GeometryDistance geometryDistance) {
        visitBinaryExpression(geometryDistance);
    }

}
