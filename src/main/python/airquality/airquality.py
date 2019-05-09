import argparse
import logging

import apache_beam as beam
from apache_beam import ExternalTransform
from apache_beam.coders import BytesCoder
from apache_beam.coders.coders import LengthPrefixCoder
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.portability.api.external_transforms_pb2 import ConfigValue
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import AfterProcessingTime
from apache_beam.transforms.trigger import AfterWatermark
import apache_beam.transforms.window as window


class ParseEventsFn(beam.DoFn):

  def __init__(self):
    super(ParseEventsFn, self).__init__()

  def process(self, element):
    logging.info(element)
    print element

    try:
      splitData = element.split(',')
      city = splitData[0]
      pollutionLevel = int(splitData[1])

      yield city, pollutionLevel

    except:
      logging.error('Parse error on "%s"', element)


class PrintValuesFn(beam.DoFn):

  def __init__(self):
    super(PrintValuesFn, self).__init__()

  def process(self, element):
    message = u'City: {}, mean value: {}'.format(element[0], element[1])
    logging.info(message)
    print message

    yield element


def _encode_str(str_obj):
  encoded_str = str_obj.encode('utf-8')
  coder = LengthPrefixCoder(BytesCoder())
  return ConfigValue(
    coder_urn='beam:coder:string_utf8:v1',
    payload=coder.encode(encoded_str))


def run(argv=None):
  parser = argparse.ArgumentParser()
  opts, pipeline_args = parser.parse_known_args(argv)
  options = PipelineOptions(pipeline_args)

  pipeline = beam.Pipeline(options=options)

  # TODO: Pubsub is not supported. The easiest way is to use ExpansionService
  #  to use external IO transform from Java SDK in Python code

  # (pipeline | 'Read from PubSub' >> beam.io.ReadFromPubSub(
  #   subscription='projects/chromatic-idea-229612/subscriptions/beam-subscription',
  #   timestamp_attribute='timestamp')

  # (pipeline | 'Read from Text' >> beam.io.ReadFromText(
  #   '/Users/lukasz/Projects/air-quality/air_quality.txt')

  args = {
    'subscription': _encode_str('projects/chromatic-idea-229612/subscriptions/beam-subscription'),
  }
  payload = ExternalConfigurationPayload(configuration=args)

  (pipeline | 'Read from PubSub' >> ExternalTransform(
    endpoint='localhost:8097',
    urn='beam:external:java:pubsub:read:v1',
    payload=payload.SerializeToString()
  )

   # '/Users/lukasz/Projects/air-quality/air_quality.txt'
   | 'Decode String' >> beam.Map(lambda b: b.decode('utf-8'))
   | 'Parse Events' >> beam.ParDo(ParseEventsFn())
   # | 'Window' >> beam.WindowInto(windowfn=window.FixedWindows(10),
   #                               trigger=AfterWatermark(
   #                                 early=AfterProcessingTime(5)),
   #                               accumulation_mode=AccumulationMode.ACCUMULATING)
   | 'Combine' >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
   # | 'Print mean value per city' >> beam.ParDo(PrintValuesFn())

   # TODO: This step required adding the folowing in JobServer's DockerEnvironmentFactory
   # .add("--volume=/Users/lukasz/Projects/air-quality:/Users/lukasz/Projects/air-quality")
   | 'write' >> beam.io.WriteToText('/Users/lukasz/Projects/air-quality/result.txt'))

  result = pipeline.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
run()
