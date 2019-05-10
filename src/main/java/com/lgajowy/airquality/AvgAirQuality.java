package com.lgajowy.airquality;

import static org.joda.time.Duration.*;
import static org.joda.time.Duration.standardSeconds;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvgAirQuality {

  private static Logger LOG = LoggerFactory.getLogger(AvgAirQuality.class);

  public interface Options extends PipelineOptions {

    String getTopic();

    void setTopic(String topic);

  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply("Read events", PubsubIO.readStrings()
        .withTimestampAttribute("timestamp")
        .fromTopic(options.getTopic()))
        .apply("Parse content", ParDo.of(new ParseEvents()))
        .apply("Window", Window.<KV<String, Integer>>into(FixedWindows.of(standardSeconds(10)))
            .triggering(AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterProcessingTime
                    .pastFirstElementInPane()
                    .plusDelayOf(standardSeconds(5))))
            .withAllowedLateness(ZERO)
            .accumulatingFiredPanes())
        .apply("Mean for each city", Mean.perKey())
        .apply("Print results", ParDo.of(new PrintResults()));

    pipeline.run().waitUntilFinish();
  }

  private static class PrintResults extends DoFn<KV<String, Double>, KV<String, Double>> {

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      KV<String, Double> result = c.element();

      String message = String
          .format("%9s | %5s | %s", result.getKey(), result.getValue().intValue(),
              window.maxTimestamp().toDateTime().toString("HH:mm:ss"));

      System.out.println(message);
      LOG.info(message);

      c.output(c.element());
    }
  }

  private static class ParseEvents extends DoFn<String, KV<String, Integer>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      String eventData = c.element();
      try {
        String[] splitData = eventData.split(",");
        String city = splitData[0].trim();
        Integer pollutionLevel = Integer.valueOf(splitData[1].trim());
        c.output(KV.of(city, pollutionLevel));

      } catch (Exception e) {
        LOG.error(
            String.format("Parse error while processing event. Error message: %s", e.getMessage()));
      }
    }
  }
}
