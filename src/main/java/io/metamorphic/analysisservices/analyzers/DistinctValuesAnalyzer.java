package io.metamorphic.analysisservices.analyzers;

import org.eobjects.analyzer.beans.api.*;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.result.Crosstab;
import org.eobjects.analyzer.result.CrosstabDimension;
import org.eobjects.analyzer.result.CrosstabNavigator;
import org.eobjects.analyzer.storage.InMemoryRowAnnotationFactory;
import org.eobjects.analyzer.storage.RowAnnotationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by markmo on 31/07/2015.
 */
@AnalyzerBean("Distinct values analyzer")
@Description("Metrics include top 5 values.")
@Concurrent(true)
public class DistinctValuesAnalyzer implements Analyzer<DistinctValuesAnalyzerResult> {

    public static final String DIMENSION_MEASURES = "Measures";
    public static final String DIMENSION_COLUMN = "Column";

    public static final String MEASURE_TOP_5 = "Top 5";
    public static final String MEASURE_DISTINCT_VALUES = "Distinct values";
    public static final String MEASURE_DISTINCT_VALUES_COUNT = "Distinct values count";

    private static final Logger logger = LoggerFactory.getLogger(DistinctValuesAnalyzer.class);

    private final Map<InputColumn<String>, DistinctValuesColumnDelegate> _columnDelegates = new HashMap<>();

    @Configured
    @ColumnProperty(escalateToMultipleJobs=true)
    InputColumn<String>[] _columns;

    @Provided
    RowAnnotationFactory _annotationFactory;

    public DistinctValuesAnalyzer() {}

    @SafeVarargs
    public DistinctValuesAnalyzer(InputColumn<String>... columns) {
        _columns = columns;
        _annotationFactory = new InMemoryRowAnnotationFactory();
        init();
    }

    @Initialize
    public void init() {
        for (InputColumn<String> column : _columns) {
            _columnDelegates.put(column, new DistinctValuesColumnDelegate(_annotationFactory));
        }
    }

    @Override
    public void run(InputRow row, int distinctCount) {
        for (InputColumn<String> column : _columns) {
            String value = row.getValue(column);

            DistinctValuesColumnDelegate delegate = _columnDelegates.get(column);
            delegate.run(row, value, distinctCount);
        }
    }

    @Override
    public DistinctValuesAnalyzerResult getResult() {
        logger.info("getResult()");
        CrosstabDimension measureDimension = new CrosstabDimension(DIMENSION_MEASURES);
        measureDimension.addCategory(MEASURE_TOP_5);
        measureDimension.addCategory(MEASURE_DISTINCT_VALUES);
        measureDimension.addCategory(MEASURE_DISTINCT_VALUES_COUNT);
        CrosstabDimension columnDimension = new CrosstabDimension(DIMENSION_COLUMN);
        Crosstab<Serializable> crosstab = new Crosstab<>(Serializable.class, columnDimension, measureDimension);
        for (InputColumn<String> column : _columns) {
            String columnName = column.getName();

            DistinctValuesColumnDelegate delegate = _columnDelegates.get(column);

            columnDimension.addCategory(columnName);

            CrosstabNavigator<Serializable> nav = crosstab.where(columnDimension, columnName);
            nav.where(measureDimension, MEASURE_TOP_5).put(delegate.getTop5());
            nav.where(measureDimension, MEASURE_DISTINCT_VALUES).put(delegate.getDistinctValues());
            nav.where(measureDimension, MEASURE_DISTINCT_VALUES_COUNT).put(delegate.getDistinctValuesCount());
        }
        return new DistinctValuesAnalyzerResult(_columns, crosstab);
    }
}
