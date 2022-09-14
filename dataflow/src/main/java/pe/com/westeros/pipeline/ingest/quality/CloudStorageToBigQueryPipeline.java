package pe.com.westeros.pipeline.ingest.quality;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.FileNameUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import pe.com.westeros.pipeline.ingest.converter.FactoryConverter;

import java.io.IOException;
import java.util.Objects;
import java.util.TimeZone;

public class CloudStorageToBigQueryPipeline {

    public static void main(String[] args) {
        TimeZone timeZone = TimeZone.getTimeZone("America/Lima");
        TimeZone.setDefault(timeZone);
        System.setProperty("user.timezone", "America/Lima");

        PipelineOptionsFactory.register(CloudStorageToBigQueryPipelineOptions.class);
        CloudStorageToBigQueryPipelineOptions pipelineOptions =
                PipelineOptionsFactory.fromArgs(args)
                        .as(CloudStorageToBigQueryPipelineOptions.class);

        run(pipelineOptions);
    }

    private static PipelineResult run(CloudStorageToBigQueryPipelineOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Extract from Cloud Storage", TextIO.read()
                        .from(options.getGcslake())
                )
                .apply("Convert to TableRow", ParDo.of(new DoFn<String, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws IOException {
                        var pipelineOptions = c.getPipelineOptions()
                                .as(CloudStorageToBigQueryPipelineOptions.class);

                        var extension = FileNameUtils.getExtension(pipelineOptions.getGcslake().get());
                        var converter = FactoryConverter.getConverter(extension);

                        var tableRow = converter.convert(c);
                        if (Objects.nonNull(tableRow)) {
                            c.output(tableRow);
                        }
                    }
                }))
                .apply(
                        "Write in Bigquery",
                        BigQueryIO.writeTableRows()
                                .withoutValidation()
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                                .to(options.getOutputTable())
                );

        return pipeline.run();
    }

}
