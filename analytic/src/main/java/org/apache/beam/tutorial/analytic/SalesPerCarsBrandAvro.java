package org.apache.beam.tutorial.analytic;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.io.File;
import java.io.IOException;

/**
 * An example of batch pipeline showing how to count number of sales per cars brand using Avro
 * records and Beam SQL transform.
 *
 * <p>Input file(s) should be a text log in the following format:
 *
 * <pre>
 *     id,brand_name,model_name,sales_number
 * </pre>
 *
 * Example of input log:
 *
 * <pre>
 *     1,Renault,Scenic,3
 *     2,Peugeut,307,2
 *     1,Renault,Megane,4
 *     3,Citroen,c3,5
 *     3,Citroen,c5,3
 * </pre>
 *
 * Example of output result:
 *
 * <pre>
 *     Citroen: 8
 *     Renault: 7
 *     Peugeut: 2
 * </pre>
 */
public class SalesPerCarsBrandAvro {

  private static final String SCHEMA_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"AvroGeneratedCarsSales\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"id\", \"type\": \"int\"},\n"
          + "     {\"name\": \"brand_name\", \"type\": \"string\"},\n"
          + "     {\"name\": \"model_name\", \"type\": \"string\"},\n"
          + "     {\"name\": \"sales_number\", \"type\": \"int\"}\n"
          + " ]\n"
          + "}";

  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

  public interface Options extends PipelineOptions {
    @Description("Input Path")
    @Default.String("/tmp/sales.avro")
    String getInputPath();

    void setInputPath(String value);
  }

  public static final void main(String args[]) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    /* Create record/row */
    PCollection<GenericRecord> records =
        pipeline.apply(
            AvroIO.readGenericRecords(SCHEMA).withBeamSchemas(true).from(options.getInputPath()));

    /* SQL Transform */
    records
        .apply(
            SqlTransform.query(
                "SELECT SUM(sales_number), brand_name FROM PCOLLECTION GROUP BY brand_name"))
        .apply(
            "Output",
            MapElements.via(
                new SimpleFunction<Row, Row>() {

                  @Override
                  public Row apply(Row input) {
                    System.out.println("OUTPUT: " + input.getValues());
                    return input;
                  }
                }));

    pipeline.run().waitUntilFinish();
  }
}
