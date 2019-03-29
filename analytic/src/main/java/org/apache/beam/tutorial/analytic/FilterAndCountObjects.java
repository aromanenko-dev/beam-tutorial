package org.apache.beam.tutorial.analytic;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

public class FilterAndCountObjects {

  public static final void main(String[] args) throws Exception {

    final FilterObjects.FilterObjectsOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(FilterObjects.FilterObjectsOptions.class);

    Pipeline pipeline = Pipeline.create(options);
pipeline
    .apply(
        KafkaIO.<Long, String>read()
            .withBootstrapServers(options.getBootstrap())
            .withTopic(options.getInputTopic())
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(StringDeserializer.class))
    .apply(
        Window.<KafkaRecord<Long, String>>into(
            FixedWindows.of(Duration.standardSeconds(10))))
        .apply(
            ParDo.of(
                new DoFn<KafkaRecord<Long, String>, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext processContext) {
                    KafkaRecord<Long, String> record = processContext.element();
                    processContext.output(record.getKV().getValue());
                  }
                }))
        .apply(
            "FilterValidCoords",
            Filter.by(
                new FilterObjects.FilterObjectsByCoordinates(
                    options.getCoordX(), options.getCoordY())))
.apply(
    ParDo.of(
        new DoFn<String, KV<Long, String>>() {
          @ProcessElement
          public void processElement(ProcessContext processContext) {
            String payload = processContext.element();
            String[] split = payload.split(",");
            if (split.length < 3) {
              return;
            }
            Long id = Long.valueOf(split[0]);

            processContext.output(KV.of(id, payload));
          }
        }))
.apply(GroupByKey.<Long, String>create())
.apply(
    ParDo.of(
        new DoFn<KV<Long, Iterable<String>>, KV<Long, Long>>() {
          @ProcessElement
          public void processElement(ProcessContext processContext) {
            Long id = processContext.element().getKey();
            long count = 0;
            for (String entry : processContext.element().getValue()) {
              count++;
            }
            processContext.output(KV.of(id, count));
          }
        }))
        .apply(
            "ExtractPayload",
            ParDo.of(
                new DoFn<KV<Long, Long>, KV<String, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) throws Exception {
                    String value = c.element().getKey() + ": " + c.element().getValue();
                    c.output(KV.of("counted", value));
                  }
                }))
        .apply(
            "WriteToKafka",
            KafkaIO.<String, String>write()
                .withBootstrapServers(options.getBootstrap())
                .withTopic(options.getOutputTopic())
                .withKeySerializer(org.apache.kafka.common.serialization.StringSerializer.class)
                .withValueSerializer(org.apache.kafka.common.serialization.StringSerializer.class));

    pipeline.run();
  }
}
