package org.apache.beam.tutorial.config;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

import java.util.Properties;

public class ConfigExample {

  public static final void main(String args[]) throws Exception {
    Properties prop = new Properties();
    prop.setProperty("key", "value");

    final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> input = pipeline.apply(TextIO.read().from("/tmp/input.data"));
    PCollection<String> output =
        input.apply(
            "Pass config as object",
            ParDo.of(new ApplyConfigDoFn(prop)));

    output.apply(TextIO.write().to("/tmp/output.data").withoutSharding());

    pipeline.run();
  }

  private static class ApplyConfigDoFn extends DoFn<String, String> {
    private Properties config;

    ApplyConfigDoFn(Properties prop) {
      this.config = prop;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(config.getProperty("key") + " : " + context.element());
    }
  }
}
