package io.metamorphic.analysisservices.transformers;

import io.metamorphic.commons.utils.DateParser;
import org.eobjects.analyzer.beans.api.Configured;
import org.eobjects.analyzer.beans.api.OutputColumns;
import org.eobjects.analyzer.beans.api.Transformer;
import org.eobjects.analyzer.beans.api.TransformerBean;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.InputRow;

import java.util.Date;

/**
 * User: markmo
 * Date: 28/03/13
 * Time: 5:24 PM
 */
@TransformerBean("String to Date")
public class DateTransformer implements Transformer<Date> {

    @Configured
    InputColumn<String>[] columns;

    private DateParser dateParser = new DateParser();

    @Override
    public OutputColumns getOutputColumns() {
        String[] names = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            InputColumn<String> column = columns[i];
            String name = column.getName() + " (as date)";
            names[i] = name;
        }
        return new OutputColumns(names);
    }

    @Override
    public Date[] transform(InputRow inputRow) {
        Date[] result = new Date[columns.length];
        for (int i = 0; i < columns.length; i++) {
            InputColumn<String> column = columns[i];
            String value = inputRow.getValue(column);
            if (value != null && (value = value.trim()).length() > 0) {
                try {
                    result[i] = dateParser.parse(value);
                } catch (IllegalArgumentException e) {
                    // TODO
                    // log exception
                }
            }
        }
        return result;
    }
}
