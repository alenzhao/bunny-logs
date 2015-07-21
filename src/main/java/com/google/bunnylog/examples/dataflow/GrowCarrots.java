package com.google.bunnylog.examples.dataflow;

import com.google.bunnylog.logger.BunnyLog;
import com.google.bunnylog.logger.DoFnWLog;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import org.apache.logging.log4j.LogManager;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


/**
 * Parallel "grow" operation
 */
public class GrowCarrots implements Serializable {
  private static final long serialVersionUID = 1L;

  public static ParDo.Bound<Integer,Integer> by(final int growthFactor) {
    return ParDo.of(new DoFnWLog<Integer, Integer>("growCarrots") {
        @Override
        public void processElement(ProcessContext c) throws InterruptedException {
          Integer input = c.element();
          // grow the carrot
          Thread.sleep(5000);
          Integer output = input * growthFactor;
          c.output(output);
        }
    });
  }
}