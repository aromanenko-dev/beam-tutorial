package org.apache.beam.tutorial.analytic;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

/**
 * An example of batch pipeline showing how to count number of sales per cars brand.
 * Input file(s) should be a text log in the following format:
 * <pre>
 *     id,brand_name,model_name,sales_number
 * </pre>
 *
 * Example of input log:
 * <pre>
 *     1,Renault,Scenic,3
 *     2,Peugeut,307,2
 *     1,Renault,Megane,4
 *     3,Citroen,c3,5
 *     3,Citroen,c5,3
 * </pre>
 *
 * Example of output result:
 * <pre>
 *     Citroen: 8
 *     Renault: 7
 *     Peugeut: 2
 * </pre>
 *
 */
public class SalesPerCarsBrand {

  public static final void main(String args[]) throws Exception {
    Pipeline pipeline = Pipeline.create();

    pipeline
        .apply(TextIO.read().from("/tmp/beam/cars_sales_log"))
        .apply("ParseAndConvertToKV", MapElements.via(new SimpleFunction<String, KV<String, Integer>>() {
          @Override
          public KV<String, Integer> apply(String input) {
            String[] split = input.split(",");
            if (split.length < 4) {
              return null;
            }
            String key = split[1];
            Integer value = Integer.valueOf(split[3]);
            return KV.of(key, value);
          }
        }))
        .apply(GroupByKey.<String, Integer>create())
        .apply("SumUpValuesByKey", ParDo.of(new DoFn<KV<String, Iterable<Integer>>, String>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            Integer totalSells = 0;
            String brand = context.element().getKey();
            Iterable<Integer> sells = context.element().getValue();
            for (Integer amount : sells) {
              totalSells += amount;
            }
            context.output(brand + ": " + totalSells);
          }
        }))
        .apply(TextIO.write().to("/tmp/beam/cars_sales_report").withoutSharding());

    pipeline.run();
  }
}