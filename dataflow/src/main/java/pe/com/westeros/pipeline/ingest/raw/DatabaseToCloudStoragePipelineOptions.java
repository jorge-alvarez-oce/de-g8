package pe.com.westeros.pipeline.ingest.raw;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface DatabaseToCloudStoragePipelineOptions extends PipelineOptions {

    @Description("The JDBC driver class name. " + "for example: com.mysql.jdbc.Driver")
    @Default.String("com.mysql.cj.jdbc.Driver")
    ValueProvider<String> getDriverClassName();

    void setDriverClassName(ValueProvider<String> driverClassName);

    @Description("The JDBC connection URL string. " + "for example: jdbc:mysql://some-host:3306/sampledb")
    ValueProvider<String> getConnectionURL();

    void setConnectionURL(ValueProvider<String> connectionURL);

    @Description("JDBC connection user name. ")
    ValueProvider<String> getUsername();

    void setUsername(ValueProvider<String> username);

    @Description("JDBC connection password. ")
    ValueProvider<String> getPassword();

    void setPassword(ValueProvider<String> password);

    @Description("Source data query string. " + "for example: select * from sampledb.sample_table")
    ValueProvider<String> getQuery();

    void setQuery(ValueProvider<String> query);

    @Description("Output Cloud storage Lake. ")
    @Default.String("gs://my-bucket/input.csv")
    ValueProvider<String> getGcslake();

    void setGcslake(ValueProvider<String> gcinput);

}
