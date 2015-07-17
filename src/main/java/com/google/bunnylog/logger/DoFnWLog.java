package com.google.bunnylog.logger;

import org.slf4j.LoggerFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;


/**
 * A DoFn that automatically logs its start and end.
 * When subclassing, if you want to write your own start/finishBundle then
 * make sure to call super.startBundle() and super.finishBundle().
 */
public abstract class DoFnWLog<T,U> extends DoFn<T,U> {
  private static final long serialVersionUID = 1L;

  protected String className;
  protected String opName;
  protected transient BunnyLog bunny;

  public DoFnWLog(String name) {
    this.opName = this.className = name;
  }

  public DoFnWLog(String className, String operationName) {
    this.className = className;
    this.opName = operationName;
  }

  @Override
  public void startBundle(DoFn<T, U>.Context c) throws Exception {
    bunny = new BunnyLog(LoggerFactory.getLogger(className));
    bunny.start(opName);
  }

  @Override
  public void finishBundle(DoFn<T, U>.Context c) throws Exception {
    bunny.end();
  }
}