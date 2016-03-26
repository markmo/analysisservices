package io.metamorphic.analysisservices.transformers;

import com.google.common.base.Joiner;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ling.CoreAnnotations.AnswerAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import io.metamorphic.commons.utils.Inflector;
import org.eobjects.analyzer.beans.api.Configured;
import org.eobjects.analyzer.beans.api.OutputColumns;
import org.eobjects.analyzer.beans.api.Transformer;
import org.eobjects.analyzer.beans.api.TransformerBean;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.InputRow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: markmo
 * Date: 28/03/13
 * Time: 5:24 PM
 */
@TransformerBean("Text to Entities")
public class EntityRecognitionTransformer implements Transformer<String> {

    static String[] entityTypes = new String[]{
            "LOCATION", "TIME", "PERSON", "ORGANIZATION", "MONEY", "PERCENT", "DATE"
    };

    @Configured
    InputColumn<String>[] columns;

    @Configured
    AbstractSequenceClassifier<CoreLabel> classifier;

    @Override
    public OutputColumns getOutputColumns() {
        String[] names = new String[columns.length * entityTypes.length];
        for (int i = 0; i < columns.length; i++) {
            for (int j = 0; j < entityTypes.length; j++) {
                InputColumn<String> column = columns[i];
                String name = column.getName() + " (" + Inflector.pluralize(entityTypes[j].toLowerCase()) + ")";
                names[i * j] = name;
            }
        }
        return new OutputColumns(names);
    }

    @Override
    public String[] transform(InputRow inputRow) {
        String[] result = new String[columns.length * entityTypes.length];
        for (int i = 0; i < columns.length; i++) {
            InputColumn<String> column = columns[i];
            String value = inputRow.getValue(column);
            if (value != null && (value = value.trim()).length() > 0) {
                List<List<CoreLabel>> labels = classifier.classify(value);
                Map<String, List<String>> entities = new HashMap<>();
                String lastAnnotation = "O";
                for (List<CoreLabel> sentence : labels) {
                    for (CoreLabel word : sentence) {
                        String annotation = word.get(AnswerAnnotation.class);
                        if (!"O".equals(annotation)) {
                            List<String> words = entities.get(annotation);
                            if (words == null) {
                                words = new ArrayList<>();
                            }
                            if (lastAnnotation.equals(annotation)) {
                                int j = words.size() - 1;
                                words.set(j, words.get(j) + " " + word.word());
                            } else {
                                words.add(word.word());
                            }
                            entities.put(annotation, words);
                            lastAnnotation = annotation;
                        }
                    }
                }
                for (int j = 0; j < entityTypes.length; j++) {
                    List<String> words = entities.get(entityTypes[j]);
                    result[i * j] = Joiner.on(",").join(words);
                }
            }
        }
        return result;
    }
}
