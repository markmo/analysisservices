package io.metamorphic.analysisservices;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import io.metamorphic.analysiscommons.models.DatabaseConnection;
import io.metamorphic.analysiscommons.models.DatasetMetrics;
import io.metamorphic.analysiscommons.models.Metric;
import io.metamorphic.analysiscommons.models.RenderedResult;
import io.metamorphic.analysisservices.analyzers.DistinctValuesAnalyzer;
import io.metamorphic.analysisservices.analyzers.DistinctValuesAnalyzerResult;
import io.metamorphic.analysisservices.transformers.DateTransformer;
import io.metamorphic.analysisservices.transformers.EntityRecognitionTransformer;
import io.metamorphic.analysisservices.transformers.IntegerTransformer;
import io.metamorphic.analysisservices.writers.EmbedHtmlAnalysisResultWriter;
import io.metamorphic.commons.Pair;
import io.metamorphic.commons.PairListBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.DataContextFactory;
import org.apache.metamodel.pojo.ArrayTableDataProvider;
import org.apache.metamodel.pojo.TableDataProvider;
import org.apache.metamodel.schema.*;
import org.apache.metamodel.util.SimpleTableDef;
import org.eobjects.analyzer.beans.*;
import org.eobjects.analyzer.beans.api.Analyzer;
import org.eobjects.analyzer.beans.api.Transformer;
import org.eobjects.analyzer.beans.convert.ConvertToBooleanTransformer;
import org.eobjects.analyzer.beans.convert.ConvertToNumberTransformer;
import org.eobjects.analyzer.beans.stringpattern.PatternFinderAnalyzer;
import org.eobjects.analyzer.beans.stringpattern.PatternFinderResult;
import org.eobjects.analyzer.beans.stringpattern.PatternFinderResultHtmlRenderer;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfigurationImpl;
import org.eobjects.analyzer.connection.Datastore;
import org.eobjects.analyzer.connection.JdbcDatastore;
import org.eobjects.analyzer.connection.PojoDatastore;
import org.eobjects.analyzer.data.MetaModelInputColumn;
import org.eobjects.analyzer.data.MutableInputColumn;
import org.eobjects.analyzer.descriptors.Descriptors;
import org.eobjects.analyzer.descriptors.SimpleDescriptorProvider;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.job.ComponentJob;
import org.eobjects.analyzer.job.builder.AnalysisJobBuilder;
import org.eobjects.analyzer.job.builder.AnalyzerJobBuilder;
import org.eobjects.analyzer.job.builder.TransformerJobBuilder;
import org.eobjects.analyzer.job.runner.AnalysisResultFuture;
import org.eobjects.analyzer.job.runner.AnalysisRunner;
import org.eobjects.analyzer.job.runner.AnalysisRunnerImpl;
import org.eobjects.analyzer.result.*;
import org.eobjects.analyzer.result.renderer.AnnotatedRowsHtmlRenderer;
import org.eobjects.analyzer.result.renderer.CrosstabHtmlRenderer;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.apache.metamodel.schema.ColumnType.*;
import static org.eobjects.analyzer.beans.BooleanAnalyzer.*;
import static org.eobjects.analyzer.beans.DateAndTimeAnalyzer.*;
import static org.eobjects.analyzer.beans.NumberAnalyzer.*;
import static org.eobjects.analyzer.beans.StringAnalyzer.*;


/**
 * Created by markmo on 4/07/2015.
 */
public class AnalysisServiceImpl implements AnalysisService {

    private static final Log log = LogFactory.getLog(AnalysisServiceImpl.class);

    private static final String[] STRING_ANALYZER_MEASURES = new String[] {
            MEASURE_AVG_CHARS, MEASURE_AVG_WHITE_SPACES, MEASURE_BLANK_COUNT, MEASURE_DIACRITIC_CHARS,
            MEASURE_DIGIT_CHARS, MEASURE_ENTIRELY_LOWERCASE_COUNT, MEASURE_ENTIRELY_UPPERCASE_COUNT, MEASURE_LOWERCASE_CHARS,
            MEASURE_MAX_CHARS, MEASURE_MAX_WHITE_SPACES, MEASURE_MAX_WORDS, MEASURE_MIN_CHARS, MEASURE_MIN_WHITE_SPACES,
            MEASURE_MIN_WORDS, MEASURE_NON_LETTER_CHARS, StringAnalyzer.MEASURE_NULL_COUNT, StringAnalyzer.MEASURE_ROW_COUNT, MEASURE_TOTAL_CHAR_COUNT,
            MEASURE_UPPERCASE_CHARS, MEASURE_UPPERCASE_CHARS_EXCL_FIRST_LETTERS, MEASURE_WORD_COUNT
    };

    private static final String[] NUMBER_ANALYZER_MEASURES = new String[] {
            MEASURE_GEOMETRIC_MEAN, MEASURE_HIGHEST_VALUE, NumberAnalyzer.MEASURE_KURTOSIS, MEASURE_LOWEST_VALUE,
            NumberAnalyzer.MEASURE_MEAN, NumberAnalyzer.MEASURE_MEDIAN, NumberAnalyzer.MEASURE_NULL_COUNT, NumberAnalyzer.MEASURE_PERCENTILE25, NumberAnalyzer.MEASURE_PERCENTILE75,
            NumberAnalyzer.MEASURE_ROW_COUNT, MEASURE_SECOND_MOMENT, NumberAnalyzer.MEASURE_SKEWNESS, MEASURE_STANDARD_DEVIATION,
            MEASURE_SUM, MEASURE_SUM_OF_SQUARES, MEASURE_VARIANCE
    };

    private static final String[] DATE_AND_TIME_ANALYZER_MEASURES = new String[] {
            MEASURE_HIGHEST_DATE, MEASURE_HIGHEST_TIME, DateAndTimeAnalyzer.MEASURE_KURTOSIS, MEASURE_LOWEST_DATE,
            MEASURE_LOWEST_TIME, DateAndTimeAnalyzer.MEASURE_MEAN, DateAndTimeAnalyzer.MEASURE_MEDIAN, DateAndTimeAnalyzer.MEASURE_NULL_COUNT,
            DateAndTimeAnalyzer.MEASURE_PERCENTILE25, DateAndTimeAnalyzer.MEASURE_PERCENTILE75, DateAndTimeAnalyzer.MEASURE_ROW_COUNT, DateAndTimeAnalyzer.MEASURE_SKEWNESS
    };

    private static final String[] BOOLEAN_ANALYZER_MEASURES = new String[] {
            MEASURE_FALSE_COUNT,
            //MEASURE_LEAST_FREQUENT, MEASURE_MOST_FREQUENT,
            BooleanAnalyzer.MEASURE_NULL_COUNT,
            BooleanAnalyzer.MEASURE_ROW_COUNT, MEASURE_TRUE_COUNT
    };

    private static final String[] DISTINCT_VALUES_ANALYZER_MEASURES = new String[] {
            DistinctValuesAnalyzer.MEASURE_TOP_5, DistinctValuesAnalyzer.MEASURE_DISTINCT_VALUES,
            DistinctValuesAnalyzer.MEASURE_DISTINCT_VALUES_COUNT
    };

    private static Map<ColumnType, String> typeSuffixMap;

    private static AbstractSequenceClassifier<CoreLabel> classifier;

    static {
        typeSuffixMap = new HashMap<>();
        typeSuffixMap.put(INTEGER, " (as int)");
        typeSuffixMap.put(DECIMAL, " (as number)");
        typeSuffixMap.put(DATE, " (as date)");
        typeSuffixMap.put(BOOLEAN, " (as boolean)");
        String serializedClassifier = "classifiers/english.all.3class.distsim.crf.ser.gz";
        classifier = CRFClassifier.getClassifierNoExceptions(serializedClassifier);
    }

    private static final Set<ColumnType> descriptiveStatisticTypes = new HashSet<ColumnType>() {{
        add(INTEGER);
        add(DECIMAL);
        add(DATE);
    }};

    public String[] getTableNames(DatabaseConnection conn) {
        JdbcDatastore datastore = createJdbcDatastore(conn.getDbName(), conn.getJdbcUrl(), conn.getUsername(), conn.getPassword());
        Table[] tables = getJdbcTables(datastore, conn.getSchema());
        if (tables == null) return null;
        List<String> tableNames = new ArrayList<>();
        for (Table table : tables) {
            tableNames.add(table.getName());
        }
        return tableNames.toArray(new String[tables.length]);
    }

    public DatasetMetrics analyze(String filename, List<List<String>> rows, List<String> columnNames, List<String> columnTypeNames) {
        return analyze(filename, rows, columnNames, columnTypeNames, false);
    }

    public List<DatasetMetrics> analyze(String sourceName, DatabaseConnection conn, List<String> tables) {
        List<DatasetMetrics> analyses = new ArrayList<>();
        for (String tableName : tables) {
            analyses.add(analyze(sourceName, conn, tableName, false));
        }
        return analyses;
    }

    public DatasetMetrics analyze(String sourceName, DatabaseConnection conn, String tableName) {
        return analyze(sourceName, conn, tableName, false);
    }

    public DatasetMetrics analyze(String sourceName, DatabaseConnection conn, String tableName, boolean includeRenderedResult) {
        if (log.isDebugEnabled()) {
            log.debug("Running analysis on " + tableName);
            log.debug("Using connection " + conn);
        }
        JdbcDatastore datastore = createJdbcDatastore(sourceName, conn.getJdbcUrl(), conn.getUsername(), conn.getPassword());
        Column[] columns = getJdbcColumns(datastore, conn.getSchema(), tableName);
        if (columns == null) return null;
        List<String> columnNames = new ArrayList<>();
        List<String> columnTypeNames = new ArrayList<>();
        for (Column column : columns) {
            columnNames.add(column.getQualifiedLabel());
            columnTypeNames.add(column.getType().getName());
        }
        List<ColumnType> columnTypes = getColumnTypes(columnTypeNames);
        if (log.isDebugEnabled()) {
            log.debug("Table: " + tableName);
            log.debug("Columns: " + columnNames);
            log.debug("Types: " + columnTypes);
        }
        DatasetMetrics datasetMetrics = new DatasetMetrics(tableName, "TABLE");
        return analyze(columns[0].getTable().getQualifiedLabel(), datasetMetrics, datastore, columnNames, columnTypes, false, includeRenderedResult);
        //return analyze(tableName, datastore, columnNames, columnTypes, includeRenderedResult);
    }

    public DatasetMetrics analyze(String filename, List<List<String>> rows, List<String> columnNames, List<String> columnTypeNames, boolean includeRenderedResult) {
        List<ColumnType> columnTypes = getColumnTypes(columnTypeNames);
        Datastore datastore = createPojoDatastore(filename, rows, columnNames, columnTypes);
        DatasetMetrics datasetMetrics = new DatasetMetrics(filename, "FILE");
        return analyze(filename, datasetMetrics, datastore, columnNames, columnTypes, true, includeRenderedResult);
    }

    private DatasetMetrics analyze(String sourceName, DatasetMetrics datasetMetrics, Datastore datastore, List<String> columnNames, List<ColumnType> columnTypes, boolean isFile, boolean includeRenderedResult) {
        SimpleDescriptorProvider descriptorProvider = new SimpleDescriptorProvider();
        descriptorProvider.addRendererBeanDescriptor(Descriptors.ofRenderer(CrosstabHtmlRenderer.class));
        descriptorProvider.addRendererBeanDescriptor(Descriptors.ofRenderer(PatternFinderResultHtmlRenderer.class));
        descriptorProvider.addRendererBeanDescriptor(Descriptors.ofRenderer(AnnotatedRowsHtmlRenderer.class));
        AnalyzerBeansConfiguration conf = (new AnalyzerBeansConfigurationImpl()).replace(descriptorProvider);
        AnalysisJobBuilder analysisJobBuilder = new AnalysisJobBuilder(conf);
        analysisJobBuilder.setDatastore(datastore);
        analysisJobBuilder.addSourceColumns(columnNames.toArray(new String[columnNames.size()]));
        Set<ColumnType> distinctColumnTypes = new HashSet<>(columnTypes);
        Map<ColumnType, List<AnalyzerJobBuilder>> analyzerJobBuilderMap = createAnalyzerJobBuilderMap(analysisJobBuilder, distinctColumnTypes,
                new PairListBuilder<ColumnType, Class<? extends Analyzer>>()
                        .add(NVARCHAR, StringAnalyzer.class)
                        .add(NVARCHAR, DistinctValuesAnalyzer.class)
                        .add(INTEGER, NumberAnalyzer.class)
                        .add(BIGINT, NumberAnalyzer.class)
                        .add(DECIMAL, NumberAnalyzer.class)
                        .add(DATE, DateAndTimeAnalyzer.class)
                        .add(BOOLEAN, BooleanAnalyzer.class)
                        .asList());
        Map<ColumnType, TransformerJobBuilder> transformerJobBuilderMap = null;
        Pair<ColumnType, TransformerJobBuilder> addTransformerJobBuilder = null;

        // Database tables are already typed
        if (isFile) {
            transformerJobBuilderMap = createTransformerJobBuilderMap(analysisJobBuilder, distinctColumnTypes,
                    new PairListBuilder<ColumnType, Class<? extends Transformer>>()
                            .add(INTEGER, IntegerTransformer.class)
                            .add(BIGINT, ConvertToNumberTransformer.class)
                            .add(DECIMAL, ConvertToNumberTransformer.class)
                            .add(DATE, DateTransformer.class)
                            .add(BOOLEAN, ConvertToBooleanTransformer.class)
                            .asList());

            if (columnTypes.contains(LONGNVARCHAR)) {
                TransformerJobBuilder entityTransformerJobBuilder = analysisJobBuilder.addTransformer(EntityRecognitionTransformer.class);
                entityTransformerJobBuilder.setConfiguredProperty("Classifier", classifier);
                addTransformerJobBuilder = new Pair<>(LONGNVARCHAR, entityTransformerJobBuilder);
            }
            List<ColumnType> originalColumnTypes = new ArrayList<>();
            for (int i = 0; i < columnNames.size(); i++) {
                originalColumnTypes.add(NVARCHAR);
            }
            addAnalyzers(analysisJobBuilder, sourceName, columnNames, originalColumnTypes, analyzerJobBuilderMap);
            AnalyzerJobBuilder patternAnalyzerBuilder = analysisJobBuilder.addAnalyzer(PatternFinderAnalyzer.class);
            MutableTable table = new MutableTable(sourceName);
            for (int i = 0; i < columnNames.size(); i++) {
                patternAnalyzerBuilder.addInputColumn(
                        new MetaModelInputColumn(
                                new MutableColumn(getUnqualifiedName(columnNames.get(i)), NVARCHAR, table, i, true)));
                                //new MutableColumn(columnNames.get(i), NVARCHAR, table, i, true)));
            }
            if (addTransformerJobBuilder == null) {
                addTransformers(analysisJobBuilder, sourceName, columnNames, columnTypes, analyzerJobBuilderMap, transformerJobBuilderMap);
            } else {
                transformerJobBuilderMap.put(addTransformerJobBuilder.l, addTransformerJobBuilder.r);
                addTransformers(analysisJobBuilder, sourceName, columnNames, columnTypes, analyzerJobBuilderMap, transformerJobBuilderMap);
            }
        } else {
            addAnalyzers(analysisJobBuilder, sourceName, columnNames, columnTypes, analyzerJobBuilderMap);
        }
        AnalysisJob job = analysisJobBuilder.toAnalysisJob();
        AnalysisRunner runner = new AnalysisRunnerImpl(conf);
        AnalysisResultFuture future = runner.run(job);
        Writer stringWriter = new StringWriter();
        EmbedHtmlAnalysisResultWriter htmlResultWriter = new EmbedHtmlAnalysisResultWriter();
        future.await();
        Map<ComponentJob, AnalyzerResult> resultMap = future.getResultMap();
        AnalysisResult results = new SimpleAnalysisResult(resultMap, new Date());

        List<Pair<String, ColumnType>> columnsByType = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = getUnqualifiedName(columnNames.get(i));
            ColumnType columnType = columnTypes.get(i);
            columnsByType.add(new Pair<>(columnName, columnType));
        }

        //Map<String, Map<String, Object>> metrics = new HashMap<>();

        for (AnalyzerResult result : resultMap.values()) {
            if (result instanceof StringAnalyzerResult) {
                for (Pair<String, ColumnType> column : columnsByType) {
                    if (column.r == NVARCHAR) {
//                        Map<String, Object> columnMetrics;
//                        if (metrics.containsKey(column.l)) {
//                            columnMetrics = metrics.get(column.l);
//                        } else {
//                            columnMetrics = new HashMap<>();
//                            metrics.put(column.l, columnMetrics);
//                        }
                        for (String measure : STRING_ANALYZER_MEASURES) {
                            Object value = ((StringAnalyzerResult) result).getCrosstab()
                                    .where(StringAnalyzer.DIMENSION_COLUMN, column.l)
                                    .where(StringAnalyzer.DIMENSION_MEASURES, measure)
                                    .get();
//                            columnMetrics.put(measure, value);
                            datasetMetrics.addColumnMetric(column.l, getColumnIndex(columnNames, column.l), io.metamorphic.analysiscommons.models.ColumnType.valueOf(column.r.getName()), measure, value);
                        }
                    }
                }
            } else if (result instanceof DistinctValuesAnalyzerResult) {
                for (Pair<String, ColumnType> column : columnsByType) {
                    if (column.r == NVARCHAR) {
//                        Map<String, Object> columnMetrics;
//                        if (metrics.containsKey(column.l)) {
//                            columnMetrics = metrics.get(column.l);
//                        } else {
//                            columnMetrics = new HashMap<>();
//                            metrics.put(column.l, columnMetrics);
//                        }
                        for (String measure : DISTINCT_VALUES_ANALYZER_MEASURES) {
                            Object value = ((DistinctValuesAnalyzerResult) result).getCrosstab()
                                    .where(StringAnalyzer.DIMENSION_COLUMN, column.l)
                                    .where(StringAnalyzer.DIMENSION_MEASURES, measure)
                                    .get();
//                            columnMetrics.put(measure, value);
                            datasetMetrics.addColumnMetric(column.l, getColumnIndex(columnNames, column.l), io.metamorphic.analysiscommons.models.ColumnType.valueOf(column.r.getName()), measure, value);
                        }
                    }
                }
            } else if (result instanceof NumberAnalyzerResult) {
                for (Pair<String, ColumnType> column : columnsByType) {
                    if (column.r == INTEGER || column.r == DECIMAL) {
//                        Map<String, Object> columnMetrics;
//                        if (metrics.containsKey(column.l)) {
//                            columnMetrics = metrics.get(column.l);
//                        } else {
//                            columnMetrics = new HashMap<>();
//                            metrics.put(column.l, columnMetrics);
//                        }
                        for (String measure : NUMBER_ANALYZER_MEASURES) {
                            Object value = ((NumberAnalyzerResult) result).getCrosstab()
                                    .where(NumberAnalyzer.DIMENSION_COLUMN, isFile ? getTransformedName(column) : column.l)
                                    .where(NumberAnalyzer.DIMENSION_MEASURE, measure)
                                    .get();
//                            columnMetrics.put(measure, value);
                            datasetMetrics.addColumnMetric(column.l, getColumnIndex(columnNames, column.l), io.metamorphic.analysiscommons.models.ColumnType.valueOf(column.r.getName()), measure, value);
                        }
                    }
                }
            } else if (result instanceof DateAndTimeAnalyzerResult) {
                for (Pair<String, ColumnType> column : columnsByType) {
                    if (column.r == DATE) {
//                        Map<String, Object> columnMetrics;
//                        if (metrics.containsKey(column.l)) {
//                            columnMetrics = metrics.get(column.l);
//                        } else {
//                            columnMetrics = new HashMap<>();
//                            metrics.put(column.l, columnMetrics);
//                        }
                        for (String measure : DATE_AND_TIME_ANALYZER_MEASURES) {
                            try {
                                Object value = ((DateAndTimeAnalyzerResult) result).getCrosstab()
                                        .where(DateAndTimeAnalyzer.DIMENSION_COLUMN, isFile ? getTransformedName(column) : column.l)
                                        .where(DateAndTimeAnalyzer.DIMENSION_MEASURE, measure)
                                        .get();
//                                columnMetrics.put(measure, value);
                                datasetMetrics.addColumnMetric(column.l, getColumnIndex(columnNames, column.l), io.metamorphic.analysiscommons.models.ColumnType.valueOf(column.r.getName()), measure, value);
                            } catch (IllegalArgumentException|NullPointerException e) {
                                // skip it
                                log.warn(e.getMessage(), e);
//                                e.printStackTrace();
                            }
                        }
                    }
                }
            } else if (result instanceof BooleanAnalyzerResult) {
                for (Pair<String, ColumnType> column : columnsByType) {
                    if (column.r == BOOLEAN) {
//                        Map<String, Object> columnMetrics;
//                        if (metrics.containsKey(column.l)) {
//                            columnMetrics = metrics.get(column.l);
//                        } else {
//                            columnMetrics = new HashMap<>();
//                            metrics.put(column.l, columnMetrics);
//                        }
                        for (String measure : BOOLEAN_ANALYZER_MEASURES) {
                            try {
                                Object value = ((BooleanAnalyzerResult) result).getColumnStatisticsCrosstab()
                                        .where(BooleanAnalyzer.DIMENSION_COLUMN, isFile ? getTransformedName(column) : column.l)
                                        .where(BooleanAnalyzer.DIMENSION_MEASURE, measure)
                                        .get();
//                                columnMetrics.put(measure, value);
                                datasetMetrics.addColumnMetric(column.l, getColumnIndex(columnNames, column.l), io.metamorphic.analysiscommons.models.ColumnType.valueOf(column.r.getName()), measure, value);
                            } catch (IllegalArgumentException|NullPointerException e) {
                                // skip it
                                log.warn(e.getMessage(), e);
//                                e.printStackTrace();
                            }
                        }
                    }
                }
            } else if (result instanceof PatternFinderResult) {
                for (Pair<String, ColumnType> column : columnsByType) {
                    if (column.l.equals(((PatternFinderResult) result).getColumn().getName())) {
//                        Map<String, Object> columnMetrics;
//                        if (metrics.containsKey(column.l)) {
//                            columnMetrics = metrics.get(column.l);
//                        } else {
//                            columnMetrics = new HashMap<>();
//                            metrics.put(column.l, columnMetrics);
//                        }
                        Crosstab crosstab = ((PatternFinderResult) result).getSingleCrosstab();
                        CrosstabDimension patternDimension = crosstab.getDimension(PatternFinderAnalyzer.DIMENSION_NAME_PATTERN);
                        for (String pattern : patternDimension.getCategories()) {
                            Integer matchCount = (Integer) crosstab
                                    .where(patternDimension, pattern)
                                    .where(PatternFinderAnalyzer.DIMENSION_NAME_MEASURES, PatternFinderAnalyzer.MEASURE_MATCH_COUNT)
                                    .get();
                            String sample = (String) crosstab
                                    .where(patternDimension, pattern)
                                    .where(PatternFinderAnalyzer.DIMENSION_NAME_MEASURES, PatternFinderAnalyzer.MEASURE_SAMPLE)
                                    .get();
//                            columnMetrics.put("matchCount", matchCount);
//                            columnMetrics.put("sample", sample);
                            datasetMetrics.addColumnMetric(column.l, getColumnIndex(columnNames, column.l), io.metamorphic.analysiscommons.models.ColumnType.valueOf(column.r.getName()), "matchCount", matchCount);
                            datasetMetrics.addColumnMetric(column.l, getColumnIndex(columnNames, column.l), io.metamorphic.analysiscommons.models.ColumnType.valueOf(column.r.getName()), "sample", sample);
                        }
                    }
                }
            }
        }
        for (Pair<String, ColumnType> column : columnsByType) {
            Map<String, Metric> metricsMap = datasetMetrics.getMetricsMap(column.l);
            Metric rowCountMetric = metricsMap.get(StringAnalyzer.MEASURE_ROW_COUNT);
            if (rowCountMetric != null) {
                Object rowCount = rowCountMetric.getValue();
                if (rowCount != null && (Integer)rowCount != 0) {
                    Metric distinctCountMetric = metricsMap.get(DistinctValuesAnalyzer.MEASURE_DISTINCT_VALUES_COUNT);
                    if (distinctCountMetric != null) {
                        Object distinctCount = distinctCountMetric.getValue();
                        if (distinctCount != null) {
                            double uniqueness = (Integer)distinctCount / ((Integer)rowCount).doubleValue();
                            datasetMetrics.addColumnMetric(column.l, getColumnIndex(columnNames, column.l), io.metamorphic.analysiscommons.models.ColumnType.valueOf(column.r.getName()), "Uniqueness", uniqueness);
                        }
                    }
                    Metric nullCountMetric = metricsMap.get(StringAnalyzer.MEASURE_NULL_COUNT);
                    if (nullCountMetric != null) {
                        Object nullCount = nullCountMetric.getValue();
                        if (nullCount != null) {
                            double completeness = 1 - ((Integer)nullCount / ((Integer)rowCount).doubleValue());
                            datasetMetrics.addColumnMetric(column.l, getColumnIndex(columnNames, column.l), io.metamorphic.analysiscommons.models.ColumnType.valueOf(column.r.getName()), "Completeness", completeness);
                        }
                    }
                }
            }
        }
        if (includeRenderedResult) {
            try {
                htmlResultWriter.write(results, conf, stringWriter);
//                Map<String, Object> renderedResult = new HashMap<>();
//                renderedResult.put("html", stringWriter.toString());
//                metrics.put("renderedResult", renderedResult);
                datasetMetrics.addRenderedResult(new RenderedResult("HTML", stringWriter.toString()));
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                e.printStackTrace();
            }
            if (!future.isSuccessful()) {
                for (Throwable e : future.getErrors()) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        return datasetMetrics;
    }

    private int getColumnIndex(List<String> qualifiedColumnNames, String unqualifiedColumnName) {
        for (int i = 0; i < qualifiedColumnNames.size(); i++) {
            if (getUnqualifiedName(qualifiedColumnNames.get(i)).equals(unqualifiedColumnName)) {
                return i + 1;
            }
        }
        return 0;
    }

    private Table[] getJdbcTables(JdbcDatastore datastore, String schemaName) {
        try (Connection conn = datastore.createConnection()) {
            DataContext ctx = DataContextFactory.createJdbcDataContext(conn);
            Schema schema = ctx.getSchemaByName(schemaName);
            return schema.getTables();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        }
        return null;
    }

    private Column[] getJdbcColumns(JdbcDatastore datastore, String schemaName, String tableName) {
        if (log.isDebugEnabled()) {
            log.debug("Fetching columns for " + schemaName + "." + tableName);
        }
        try (Connection conn = datastore.createConnection()) {
            DataContext ctx = DataContextFactory.createJdbcDataContext(conn);
            Schema schema = ctx.getSchemaByName(schemaName);
            Table table = schema.getTableByName(tableName);
            return table.getColumns();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        }
        return null;
    }

    private JdbcDatastore createJdbcDatastore(String name, String jdbcUrl, String username, String password) {
        if (log.isDebugEnabled()) {
            log.debug("Creating JdbcDatastore");
        }
        return new JdbcDatastore(name, jdbcUrl, "org.postgresql.Driver", username, password, true);
    }

    private Datastore createPojoDatastore(String filename,
                                          List<List<String>> rows,
                                          List<String> columnNames,
                                          List<ColumnType> columnTypes) {
        if (log.isDebugEnabled()) {
            log.debug("Creating PojoDatastore");
        }
        final TableDataProvider tableDataProvider = createDataProvider(filename, rows, columnNames, columnTypes);
        return new PojoDatastore("file", filename, tableDataProvider);
    }

    private TableDataProvider createDataProvider(String filename,
                                                 List<List<String>> rows,
                                                 List<String> columnNames,
                                                 List<ColumnType> columnTypes) {
        final Collection<Object[]> rowArrays = new ArrayList<>(rows.size());
        for (List<String> row : rows) {
            rowArrays.add(row.toArray(new String[row.size()]));
        }
        final SimpleTableDef tableDef = new SimpleTableDef(filename,
                columnNames.toArray(new String[columnNames.size()]),
                columnTypes.toArray(new ColumnType[columnTypes.size()]));
        return new ArrayTableDataProvider(tableDef, rowArrays);
    }

    private Map<ColumnType, List<AnalyzerJobBuilder>>
    createAnalyzerJobBuilderMap(AnalysisJobBuilder analysisJobBuilder,
                                Set<ColumnType> columnTypeSet,
                                List<Pair<ColumnType, Class<? extends Analyzer>>> analyzers) {
        Map<Class<? extends Analyzer>, Set<ColumnType>> analyzerMap = new HashMap<>();
        for (Pair<ColumnType, Class<? extends Analyzer>> pair : analyzers) {
            ColumnType columnType = pair.l;
            Class<? extends Analyzer> analyzer = pair.r;
            if (!analyzerMap.containsKey(analyzer)) {
                analyzerMap.put(analyzer, new HashSet<ColumnType>());
            }
            analyzerMap.get(analyzer).add(columnType);
        }
        return createAnalyzerJobBuilderMap(analysisJobBuilder, columnTypeSet, analyzerMap);
    }

    private Map<ColumnType, List<AnalyzerJobBuilder>>
    createAnalyzerJobBuilderMap(AnalysisJobBuilder analysisJobBuilder,
                                Set<ColumnType> columnTypeSet,
                                Map<Class<? extends Analyzer>, Set<ColumnType>> analyzerMap) {
        Map<ColumnType, List<AnalyzerJobBuilder>> analyzerJobBuilderMap = new HashMap<>();
        for (Map.Entry<Class<? extends Analyzer>, Set<ColumnType>> entry : analyzerMap.entrySet()) {
            Set<ColumnType> inter = intersection(entry.getValue(), columnTypeSet);
            if (!inter.isEmpty()) {
                AnalyzerJobBuilder analyzerJobBuilder = analysisJobBuilder.addAnalyzer(entry.getKey());
                Set<ColumnType> inter1 = intersection(entry.getValue(), descriptiveStatisticTypes);
                if (!inter1.isEmpty()) {
                    analyzerJobBuilder.setConfiguredProperty("Descriptive statistics", true);
                }
                for (ColumnType columnType : entry.getValue()) {
                    if (!analyzerJobBuilderMap.containsKey(columnType)) {
                        analyzerJobBuilderMap.put(columnType, new ArrayList<AnalyzerJobBuilder>());
                    }
                    analyzerJobBuilderMap.get(columnType).add(analyzerJobBuilder);
                }
            }
        }
        return analyzerJobBuilderMap;
    }

    private Map<ColumnType, TransformerJobBuilder>
    createTransformerJobBuilderMap(AnalysisJobBuilder analysisJobBuilder,
                                   Set<ColumnType> columnTypeSet,
                                   List<Pair<ColumnType, Class<? extends Transformer>>> transformers) {
        Map<Class<? extends Transformer>, Set<ColumnType>> transformerMap = new HashMap<>();
        for (Pair<ColumnType, Class<? extends Transformer>> pair : transformers) {
            ColumnType columnType = pair.l;
            Class<? extends Transformer> transformer = pair.r;
            if (!transformerMap.containsKey(transformer)) {
                transformerMap.put(transformer, new HashSet<ColumnType>());
            }
            transformerMap.get(transformer).add(columnType);
        }
        return createTransformerJobBuilderMap(analysisJobBuilder, columnTypeSet, transformerMap);
    }

    private Map<ColumnType, TransformerJobBuilder>
    createTransformerJobBuilderMap(AnalysisJobBuilder analysisJobBuilder,
                                   Set<ColumnType> columnTypeSet,
                                   Map<Class<? extends Transformer>, Set<ColumnType>> transformerMap) {
        Map<ColumnType, TransformerJobBuilder> transformerJobBuilderMap = new HashMap<>();
        for (Map.Entry<Class<? extends Transformer>, Set<ColumnType>> entry : transformerMap.entrySet()) {
            Set<ColumnType> inter = intersection(entry.getValue(), columnTypeSet);
            if (!inter.isEmpty()) {
                TransformerJobBuilder transformerJobBuilder = analysisJobBuilder.addTransformer(entry.getKey());
                for (ColumnType columnType : entry.getValue()) {
                    transformerJobBuilderMap.put(columnType, transformerJobBuilder);
                }
            }
        }
        return transformerJobBuilderMap;
    }

    private void addAnalyzers(AnalysisJobBuilder analysisJobBuilder,
                              String filename,
                              List<String> columnNames,
                              List<ColumnType> columnTypes,
                              Map<ColumnType, List<AnalyzerJobBuilder>> analyzerJobBuilderMap) {
        MutableTable table = new MutableTable(filename);
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = getUnqualifiedName(columnNames.get(i));
            ColumnType columnType = columnTypes.get(i);
            for (AnalyzerJobBuilder analyzerJobBuilder : analyzerJobBuilderMap.get(columnType)) {
                MutableColumn column = new MutableColumn(columnName, columnType, table, i, true);
                if (log.isDebugEnabled()) {
                    log.debug(column);
                }
                analyzerJobBuilder.addInputColumn(
                        new MetaModelInputColumn(column));
                if (log.isDebugEnabled()) {
                    log.debug("added to " + analyzerJobBuilder.getDescriptor().getDisplayName());
                }
            }
        }
    }

    private void addTransformers(AnalysisJobBuilder analysisJobBuilder,
                                 String filename,
                                 List<String> columnNames,
                                 List<ColumnType> columnTypes,
                                 Map<ColumnType, List<AnalyzerJobBuilder>> analyzerJobBuilderMap,
                                 Map<ColumnType, TransformerJobBuilder> transformerJobBuilderMap) {
        MutableTable table = new MutableTable(filename);
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = getUnqualifiedName(columnNames.get(i));
            ColumnType columnType = columnTypes.get(i);
            if (transformerJobBuilderMap.containsKey(columnType)) {
                TransformerJobBuilder transformerJobBuilder = transformerJobBuilderMap.get(columnType);
                transformerJobBuilder.addInputColumn(
                        new MetaModelInputColumn(
                                new MutableColumn(columnName, NVARCHAR, table, i, true)));
                for (AnalyzerJobBuilder analyzerJobBuilder : analyzerJobBuilderMap.get(columnType)) {
                    if (columnType != LONGNVARCHAR) {
                        String transformedColumnName = columnName + typeSuffixMap.get(columnType);
                        MutableInputColumn inputColumn = transformerJobBuilder.getOutputColumnByName(transformedColumnName);
                        if (log.isDebugEnabled()) {
                            log.debug(inputColumn);
                        }
                        analyzerJobBuilder.addInputColumn(inputColumn);
                        if (log.isDebugEnabled()) {
                            log.debug("added to " + analyzerJobBuilder.getDescriptor().getDisplayName());
                        }
                    }
                }
            }
        }
    }

    private List<ColumnType> getColumnTypes(List<String> columnTypeNames) {
        if (log.isDebugEnabled()) {
            log.debug("Getting column types for " + columnTypeNames);
        }
        List<ColumnType> columnTypes = new ArrayList<>();
        for (String name : columnTypeNames) {
            switch (name) {
                case "INTEGER":
                    columnTypes.add(INTEGER);
                    break;
                case "BIGINT":
                    columnTypes.add(INTEGER);
                    break;
                case "SMALLINT":
                    columnTypes.add(INTEGER);
                    break;
                case "DECIMAL":
                    columnTypes.add(DECIMAL);
                    break;
                case "DATE":
                    columnTypes.add(DATE);
                    break;
                case "TIMESTAMP":
                    columnTypes.add(DATE);
                    break;
                case "BOOLEAN":
                    columnTypes.add(BOOLEAN);
                    break;
                case "LONGNVARCHAR":
                    columnTypes.add(LONGNVARCHAR);
                    break;
                default:
                    columnTypes.add(NVARCHAR);
            }
        }
        return columnTypes;
    }

    private <T> Set<T> intersection(Set<T> a, Set<T> b) {
        Set<T> inter = new HashSet<>(a);
        inter.retainAll(b);
        return inter;
    }

    private static String getTransformedName(Pair<String, ColumnType> column) {
        return column.l + typeSuffixMap.get(column.r);
    }

    private static String getUnqualifiedName(String qualifiedName) {
        if (qualifiedName.indexOf('.') == -1) return qualifiedName;
        return qualifiedName.substring(qualifiedName.lastIndexOf('.') + 1);
    }
}
