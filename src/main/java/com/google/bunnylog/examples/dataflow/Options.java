package com.google.bunnylog.examples.dataflow;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

/**
 * Options for the Dataflow example.
 */
public interface Options extends DataflowPipelineOptions {

  @Description("Google Cloud Storage path prefix of the files to which to write pipeline output.")
  String getOutput();
  void setOutput(String output);

}
