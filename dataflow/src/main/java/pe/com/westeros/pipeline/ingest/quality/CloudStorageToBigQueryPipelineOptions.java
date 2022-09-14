package pe.com.westeros.pipeline.ingest.quality;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface CloudStorageToBigQueryPipelineOptions extends PipelineOptions {

    @Description("Output Cloud storage Lake. ")
    @Default.String("gs://my-bucket/input.csv")
    ValueProvider<String> getGcslake();

    void setGcslake(ValueProvider<String> gcslake);

    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);

    @Description("BigQuery Table spec to write the output to"
            + "for example: some-project-id:somedataset.sometable")
    ValueProvider<String> getOutputTable();

    void setOutputTable(ValueProvider<String> value);
}
