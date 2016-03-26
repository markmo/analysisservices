package io.metamorphic.analysisservices.analyzers;

import io.metamorphic.analysiscommons.models.TermFrequency;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.storage.RowAnnotation;
import org.eobjects.analyzer.storage.RowAnnotationFactory;

import java.util.*;

/**
 * Created by markmo on 31/07/2015.
 */
final class DistinctValuesColumnDelegate {

    private final RowAnnotationFactory _annotationFactory;
    private final RowAnnotation _distinctValuesAnnotation;

    private volatile Map<String, Integer> _distinctValues = new HashMap<>();

    public DistinctValuesColumnDelegate(RowAnnotationFactory annotationFactory) {
        _annotationFactory = annotationFactory;
        _distinctValuesAnnotation = annotationFactory.createAnnotation();
    }

    public synchronized void run(InputRow row, final String value, int distinctCount) {
        String val = (value == null ? "NULL" : value);
        if (_distinctValues.containsKey(val)) {
            _distinctValues.put(val, _distinctValues.get(val) + 1);
        } else {
            _distinctValues.put(val, 1);
        }
        _annotationFactory.annotate(row, distinctCount, _distinctValuesAnnotation);
    }

    protected int getDistinctValuesCount() {
        return _distinctValues.values().size();
    }

    protected TermFrequency[] getDistinctValues() {
        List<TermFrequency> sorted = sortMapByValue(_distinctValues);
        int n = Math.min(sorted.size(), 1000);
        return sorted.toArray(new TermFrequency[sorted.size()]);
    }

    protected TermFrequency[] getTop5() {
        List<TermFrequency> top5List = sortMapByValue(_distinctValues);
        int n = Math.min(top5List.size(), 5);
        return top5List.subList(0, n).toArray(new TermFrequency[n]);
    }

    private static List<TermFrequency> sortMapByValue(Map<String, Integer> map) {
        List<Map.Entry<String, Integer>> list = new LinkedList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b) {
                return a.getValue().compareTo(b.getValue()) * -1;
            }
        });
        List<TermFrequency> terms = new LinkedList<>();
        for (Map.Entry<String, Integer> entry : list) {
            terms.add(new TermFrequency(entry.getKey(), entry.getValue()));
        }
        return terms;
    }
}
