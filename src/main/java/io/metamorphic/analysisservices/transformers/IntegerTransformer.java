package io.metamorphic.analysisservices.transformers;

import org.eobjects.analyzer.beans.api.*;
import org.eobjects.analyzer.data.*;

/**
 * User: markmo
 * Date: 28/03/13
 * Time: 5:24 PM
 */
@TransformerBean("String to Integer")
public class IntegerTransformer implements Transformer<Integer> {

    @Configured
    InputColumn<String>[] columns;

    @Override
    public OutputColumns getOutputColumns() {
        String[] names = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            InputColumn<String> column = columns[i];
            String name = column.getName() + " (as int)";
            names[i] = name;
        }
        return new OutputColumns(names);
    }

    @Override
    public Integer[] transform(InputRow inputRow) {
        Integer[] result = new Integer[columns.length];
        for (int i = 0; i < columns.length; i++) {
            InputColumn<String> column = columns[i];
            String value = inputRow.getValue(column).trim();
            if (value.length() > 0) {
                try {
                    result[i] = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    // TODO
                    // log exception
                }
            }
        }
        return result;
    }
}
