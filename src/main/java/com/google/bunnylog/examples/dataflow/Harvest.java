package com.google.bunnylog.examples.dataflow;

import com.google.bunnylog.logger.BunnyLog;
import com.google.bunnylog.logger.DoFnWLog;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.apache.logging.log4j.LogManager;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


/**
 * Parallel "harvest" operation
 */
public class Harvest implements Serializable {
  private static final long serialVersionUID = 1L;

  public static ParDo.Bound<Integer, String> of() {
    return ParDo.of(new DoFnWLog<Integer, String>("harvest") {
              @Override
              public void processElement(ProcessContext c) throws InterruptedException {
                Integer input = c.element();
                String output = "carrot level " + input;
                Thread.sleep(5000);
                c.output(output);
              }
            });
  }
}