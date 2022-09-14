package pe.com.westeros.pipeline.ingest.converter;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

public interface StringToTableRowConverter {

    TableRow convert(DoFn<String, TableRow>.ProcessContext c);
}
