package com.lgajowy.airquality;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvgAirQuality {

  private static Logger LOG = LoggerFactory.getLogger(AvgAirQuality.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    System.out.println(options.toString());

    pipeline.apply("Read", PubsubIO.readStrings()
        .withTimestampAttribute("timestamp")
        .fromSubscription("projects/chromatic-idea-229612/subscriptions/beam-subscription"))
        .apply("Parse", ParDo.of(new ParseEvents()))
        .apply("Window", Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardSeconds(10)))
            .triggering(AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardSeconds(5))))
            .withAllowedLateness(Duration.ZERO)
            .accumulatingFiredPanes())
        .apply("Combine", Mean.perKey())
        .apply("Print mean value per city", MapElements.via(new SimpleFunction<KV<String, Double>, KV<String, Double>>() {
          @Override
          public KV<String, Double> apply(KV<String, Double> input) {
            LOG.info(String.format("City: %s, mean value: %s", input.getKey(), input.getValue()));
            System.out.println(String.format("City: %s, mean value: %s", input.getKey(), input.getValue()));
            return input;
          }
        }));

    pipeline.run().waitUntilFinish();
  }

  private static class ParseEvents extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String eventData = c.element();
      LOG.info(eventData);
      System.out.println(eventData);
      System.out.println(c.timestamp());

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
