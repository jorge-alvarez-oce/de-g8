package pe.com.westeros.pipeline.ingest.converter;

public class FactoryConverter {

    public static StringToTableRowConverter getConverter(String extension){
        if ("csv".equals(extension)) {
            return new CsvStringToTableRowConverter();
        } else {
            return new JsonStringToTableRowConverter();
        }
    }
}
