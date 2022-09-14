package pe.com.westeros.pipeline.ingest.converter;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JsonStringToTableRowConverter implements StringToTableRowConverter {

    public TableRow convert(DoFn<String, TableRow>.ProcessContext c) {
        byte[] inputTextInBytes = c.element().getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(inputTextInBytes);

        try {
            return TableRowJsonCoder.of()
                    .decode(inputStream, Coder.Context.OUTER);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
