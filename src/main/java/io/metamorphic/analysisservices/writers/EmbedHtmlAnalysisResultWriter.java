package io.metamorphic.analysisservices.writers;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;

import org.eobjects.analyzer.job.ComponentJob;
import org.eobjects.analyzer.result.html.HtmlAnalysisResultWriter;
import org.eobjects.analyzer.result.html.HtmlFragment;
import org.eobjects.analyzer.result.html.HtmlRenderingContext;

/**
 * User: markmo
 * Date: 2/04/13
 * Time: 11:43 AM
 */
public class EmbedHtmlAnalysisResultWriter extends HtmlAnalysisResultWriter {

    @Override
    protected void writeHtmlBegin(Writer writer, HtmlRenderingContext context) throws IOException {
        // do not write
    }

    @Override
    protected void writeHtmlEnd(Writer writer, HtmlRenderingContext context) throws IOException {
        // do not write
    }

    @Override
    protected void writeHead(final Writer writer, final Map<ComponentJob, HtmlFragment> htmlFragments,
                             HtmlRenderingContext context) throws IOException {
        // do not write
    }

    @Override
    protected void writeBodyBegin(Writer writer, HtmlRenderingContext context) throws IOException {
        //writer.write("<div class=\"analysisResultContainer\">\n");
    }

    @Override
    protected void writeBodyEnd(Writer writer, HtmlRenderingContext context) throws IOException {
        //writer.write("</div>\n");
    }
}
