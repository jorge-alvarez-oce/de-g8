package pe.com.westeros.pipeline.ingest.raw;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import pe.com.westeros.DefaultPipelineOptions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.TimeZone;

public class DatabaseToCloudStoragePipeline {

    public static void main(String[] args) {
        TimeZone timeZone = TimeZone.getTimeZone("America/Lima");
        TimeZone.setDefault(timeZone);
        System.setProperty("user.timezone", "America/Lima");

        DatabaseToCloudStoragePipelineOptions pipelineOptions =
                PipelineOptionsFactory.fromArgs(args)
                        .as(DatabaseToCloudStoragePipelineOptions.class);

        run(pipelineOptions);
    }

    private static PipelineResult run(DatabaseToCloudStoragePipelineOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Extraccion de JDBC",
                        JdbcIO.<TableRow>read()
                                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                                options.getDriverClassName(),
                                                options.getConnectionURL()
                                        )
                                        .withUsername(options.getUsername())
                                        .withPassword(options.getPassword()))
                                .withQuery(options.getQuery())
                                .withRowMapper(DefaultPipelineOptions.getResultSetToTableRow())
                )
                .apply("Proceso Cloud storage",
                        ParDo.of(new DoFn<TableRow, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws IOException {

                                var outputStream = new ByteArrayOutputStream();
                                TableRowJsonCoder.of().encode(c.element(), outputStream, Coder.Context.OUTER);

                                c.output(outputStream.toString(StandardCharsets.UTF_8));
                            }
                        }))
                .apply("Deposito en GCS",
                        TextIO.write()
                                .to(options.getGcslake())
                                .withoutSharding()
                );
        return pipeline.run();
    }
}
