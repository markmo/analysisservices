package io.metamorphic.analysisservices;

import io.metamorphic.analysiscommons.models.DatabaseConnection;
import io.metamorphic.analysiscommons.models.DatasetMetrics;

import java.util.List;

/**
 * Created by markmo on 6/07/2015.
 */
public interface AnalysisService {

    String[] getTableNames(DatabaseConnection conn);

    DatasetMetrics analyze(String filename, List<List<String>> rows, List<String> columnNames, List<String> columnTypeNames);

    DatasetMetrics analyze(String filename, List<List<String>> rows, List<String> columnNames, List<String> columnTypeNames, boolean includeRenderedResult);

    DatasetMetrics analyze(String sourceName, DatabaseConnection conn, String tableName);

    List<DatasetMetrics> analyze(String sourceName, DatabaseConnection conn, List<String> tables);

    DatasetMetrics analyze(String sourceName, DatabaseConnection conn, String tableName, boolean includeRenderedResult);
}
