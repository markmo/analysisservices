package io.metamorphic.analysisservices.analyzers;

import io.metamorphic.analysiscommons.models.TermFrequency;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.result.Crosstab;
import org.eobjects.analyzer.result.CrosstabResult;
import org.eobjects.analyzer.result.Metric;

/**
 * Created by markmo on 31/07/2015.
 */
public class DistinctValuesAnalyzerResult extends CrosstabResult {

    private static final long serialVersionUID = 1L;

    private final InputColumn<String>[] _columns;

    public DistinctValuesAnalyzerResult(InputColumn<String>[] columns, Crosstab<?> crosstab) {
        super(crosstab);
        _columns = columns;
    }

    public InputColumn<String>[] getColumns() {
        return _columns;
    }

    @Metric(DistinctValuesAnalyzer.MEASURE_TOP_5)
    public TermFrequency[] getTop5(InputColumn<?> col) {
        return (TermFrequency[]) getCrosstab().where(DistinctValuesAnalyzer.DIMENSION_COLUMN, col.getName())
                .where(DistinctValuesAnalyzer.DIMENSION_MEASURES, DistinctValuesAnalyzer.MEASURE_TOP_5).get();
    }

    @Metric(DistinctValuesAnalyzer.MEASURE_DISTINCT_VALUES)
    public TermFrequency[] getDistinctValues(InputColumn<?> col) {
        return (TermFrequency[]) getCrosstab().where(DistinctValuesAnalyzer.DIMENSION_COLUMN, col.getName())
                .where(DistinctValuesAnalyzer.DIMENSION_MEASURES, DistinctValuesAnalyzer.MEASURE_DISTINCT_VALUES).get();
    }

    @Metric(DistinctValuesAnalyzer.MEASURE_DISTINCT_VALUES_COUNT)
    public int getDistinctValuesCount(InputColumn<?> col) {
        return (Integer) getCrosstab().where(DistinctValuesAnalyzer.DIMENSION_COLUMN, col.getName())
                .where(DistinctValuesAnalyzer.DIMENSION_MEASURES, DistinctValuesAnalyzer.MEASURE_DISTINCT_VALUES_COUNT).get();
    }
}
