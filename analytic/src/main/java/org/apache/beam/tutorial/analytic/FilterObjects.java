package org.apache.beam.tutorial.analytic;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * An example of streaming job which reads unbounded data from Kafka.
 * It implements the following pipeline:
 * <pre>
 *     - Consume a message from input Kafka topic once it was arrived. Every message contains the
 *     coordinates (x,y) of point on the plot.
 *     - Filter out the points that are out of defined region on the plot.
 *     - Write filtered messages into output Kafka topic.
 * </pre>
 *
 * Format of message with coordinates:
 * <pre>
 *     id,x,y
 * </pre>
 */
public class FilterObjects {

  static final int COORD_X = 100;  // Default maximum coordinate value (axis X)
  static final int COORD_Y = 100;  // Default maximum coordinate value (axis Y)
  static final String OUTPUT_PATH = "/tmp/beam/objects_report";  // Default output path
  static final String BOOTSTRAP_SERVERS = "localhost:9092";  // Default bootstrap kafka servers
  static final String INPUT_TOPIC = "BEAM_IN";  // Default input kafka topic name
  static final String OUTPUT_TOPIC = "BEAM_OUT";  // Default output kafka topic name

  /**
   * Specific pipeline options.
   */
  private interface Options extends PipelineOptions {
    @Description("Maximum coordinate value (axis X)")
    @Default.Integer(COORD_X)
    Integer getCoordX();
    void setCoordX(Integer value);

    @Description("Maximum coordinate value (axis Y)")
    @Default.Integer(COORD_Y)
    Integer getCoordY();
    void setCoordY(Integer value);

    @Description("Kafka bootstrap servers")
    @Default.String(BOOTSTRAP_SERVERS)
    String getBootstrap();
    void setBootstrap(String value);

    @Description("Kafka input topic name")
    @Default.String(INPUT_TOPIC)
    String getInputTopic();
    void setInputTopic(String value);

    @Description("Kafka output topic name")
    @Default.String(OUTPUT_TOPIC)
    String getOutputTopic();
    void setOutputTopic(String value);

  }

  private static class FilterObjectsByCoordinates implements SerializableFunction<String, Boolean> {
    private Integer maxCoordX;
    private Integer maxCoordY;

    public FilterObjectsByCoordinates(Integer maxCoordX, Integer maxCoordY) {
      this.maxCoordX = maxCoordX;
      this.maxCoordY = maxCoordY;
    }

    public Boolean apply(String input) {
      String[] split = input.split(",");
      if (split.length < 3) {
        return false;
      }
      Integer coordX = Integer.valueOf(split[1]);
      Integer coordY = Integer.valueOf(split[2]);
      return (coordX >= 0 && coordX < this.maxCoordX
          && coordY >= 0 && coordY < this.maxCoordY);
    }
  }

  public final static void main(String[] args) throws Exception {
    final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            KafkaIO.<Long, String>read()
                .withBootstrapServers(options.getBootstrap())
                .withTopic(options.getInputTopic())
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(StringDeserializer.class))
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
            Filter.by(new FilterObjectsByCoordinates(options.getCoordX(), options.getCoordY())))

        .apply(
            "ExtractPayload",
            ParDo.of(
                new DoFn<String, KV<String, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) throws Exception {
                    c.output(KV.of("filtered", c.element()));
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
