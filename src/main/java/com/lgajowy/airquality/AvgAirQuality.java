package com.lgajowy.airquality;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvgAirQuality {

  private static Logger LOG = LoggerFactory.getLogger(AvgAirQuality.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    System.out.println(options.toString());

    pipeline.apply(PubsubIO.readStrings()
        .fromSubscription("projects/chromatic-idea-229612/subscriptions/beam-subscrption")).apply(
        MapElements.into(TypeDescriptors.strings())
            .via((SerializableFunction<String, String>) input -> {
              LOG.info(input);
              System.err.println(input);
              return input;
            }));

    pipeline.run().waitUntilFinish();
  }
}
