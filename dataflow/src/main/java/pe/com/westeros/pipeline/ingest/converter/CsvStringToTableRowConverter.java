package pe.com.westeros.pipeline.ingest.converter;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.Objects;

public class CsvStringToTableRowConverter implements StringToTableRowConverter {

    public TableRow convert(DoFn<String, TableRow>.ProcessContext c) {
        var orderDetailInPlane = c.element();

        if (Objects.isNull(orderDetailInPlane)) {
            return null;
        }
        if (orderDetailInPlane.contains("orderNumber")) {
            return null;
        }

        var orderDetailValues = orderDetailInPlane.split(";");

        var tableRow = new TableRow();
        tableRow.set("orderNumber", Integer.parseInt(cleanValue(0, orderDetailValues)));
        tableRow.set("productCode", cleanValue(1, orderDetailValues));
        tableRow.set("quantityOrdered", Integer.parseInt(cleanValue(2, orderDetailValues)));
        tableRow.set("priceEach", Double.parseDouble(cleanValue(3, orderDetailValues)));
        tableRow.set("orderLineNumber", Integer.parseInt(cleanValue(4, orderDetailValues)));

        return tableRow;
    }

    private String cleanValue(int index, String[] orderDetailValues) {
        return orderDetailValues[index].replace(";", "");
    }
}
