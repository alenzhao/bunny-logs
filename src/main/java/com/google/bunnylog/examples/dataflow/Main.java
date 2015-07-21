package com.google.bunnylog.examples.dataflow;

import com.google.bunnylog.logger.BunnyLog;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TextualIntegerCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;

import org.apache.logging.log4j.LogManager;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


/**
 * A sample dataflow program that generates a log that we can then process.
 *
 * You can run this example via the following command:
 * mvn exec:java -Dexec.mainClass="com.google.bunnylog.examples.dataflow.Main"
 */
public class Main {

  public static void main(String[] args) throws InterruptedException {
    BunnyLog bl = new BunnyLog(LogManager.getLogger());
    bl.start("main");
    PipelineOptionsFactory.register(Options.class);
    Options popts = PipelineOptionsFactory.fromArgs(args).create().as(Options.class);
    String output = popts.getOutput() + "bunny_output.txt";
    Pipeline pipeline = Pipeline.create(popts);

    System.out.println("Writing output to " + output);

    List<Integer> numbers = Arrays.asList(1, 2, 3, 4);
    pipeline
        .apply(Create.of(numbers))
        .apply(GrowCarrots.by(100))
        .apply(Harvest.of())
        .apply(TextIO.Write.to(output))
        ;

    bl.stepEnd("setup pipeline");
    pipeline.run();
    bl.stepEnd("run pipeline");
    bl.end();
    Thread.sleep(100);
    System.out.println("We're done! Next: ");
    System.out.println("  1) copy this output to log-df.txt (if you haven't already)");
    System.out.println("  2) grab the dataflow log 'worker-stdout' to worker-stdout.json");
    System.out.println("  3) run mvn exec:java -Dexec.mainClass=\"com.google.bunnylog.analyzer.Main\" -Dexec.args=\"-l log-df.txt -d worker-stdout.json\"");
    System.out.println("  4) open out.svg in your web browser (or svg editor if you have one).");
  }

}
