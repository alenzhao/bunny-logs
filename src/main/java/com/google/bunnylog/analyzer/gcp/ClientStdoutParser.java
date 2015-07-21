package com.google.bunnylog.analyzer.gcp;

import com.google.bunnylog.analyzer.core.ConcreteOperationInterval;
import com.google.bunnylog.analyzer.core.OperationInterval;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Reads the stdout of a Dataflow application (client-side).
 */
public class ClientStdoutParser {

  static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'H:m:s.S'Z:'");

  public static List<OperationInterval> parse(String fname)  throws Exception {

    BufferedReader r = new BufferedReader(new FileReader(fname));
    ArrayList<OperationInterval> ret = new ArrayList<OperationInterval>();
    String line;

    ConcreteOperationInterval job = null;
    String jobID = null;
    String opInProgress = null;
    Date opStartTime = null;

    while ((line=r.readLine())!=null) {
      try {

        // We're looking for the following sequence:
        // Submitted job: 2015-07-14_12_26_52-15432457801169333516
        // 2015-07-14T19:26:59.295Z: (52599cc9274ddb4): Basic: Starting worker pool synchronously...
        // 2015-07-14T19:31:46.412Z: (52599cc9274dde3): Basic: Worker pool is running.
        // 2015-07-14T19:32:35.720Z: S04: (5f60ac31b9f746ee): Basic: Executing operation Globally/Combine.Globally/AsIterable/CreatePCollectionView
        // 2015-07-14T19:31:47.829Z: (623062ee9246d205): Basic: Executing operation Globally/Combine.Globally/AnonymousParDo/Reshard/Close
        // 2015-07-14T19:32:38.658Z: (bc4d81d0e967ade9): Basic: Starting worker pool teardown.
        // 2015-07-14T19:33:42.023Z: (bc4d81d0e967a41e): Basic: Worker pool stopped.
        Date when;
        String[] p = line.split(" ");
        if (p.length<2) continue;

        if (line.startsWith("Submitted job: ") && p.length==3) {
          jobID = p[2];
        }

        try {
          timestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
          when = timestampFormat.parse(p[0]);
        } catch (Exception x) {
          // not the format we're looking for
          continue;
        }


        if (line.contains("Starting worker pool synchronously")) {
          job = new ConcreteOperationInterval();
          job.begin = when;
          job.attributes.put("log", "stdout");
          if (null!=jobID) {
            job.attributes.put("jobID", jobID);
            job.label = "Client for job "+jobID;
            job.group = job.label;
          } else {
            job.label = "Client of Dataflow job";
          }
        } else if (line.contains("Worker pool is running")) {
          ConcreteOperationInterval startup = new ConcreteOperationInterval();
          startup.begin = job.begin;
          startup.end = when;
          startup.label = "setup";
          addDuration(startup);
          job.sub.add(startup);
          opStartTime = when;
        } else if (line.contains("Executing operation")) {
          if (opInProgress!=null) {
            ConcreteOperationInterval op = new ConcreteOperationInterval();
            op.begin = opStartTime;
            op.end = when;
            op.label = opInProgress;
            addDuration(op);
            job.sub.add(op);
            opInProgress = null;
          }
          opInProgress = line.substring(line.indexOf("Executing operation"));
          opStartTime = when;
        } else if (line.contains("Starting worker pool teardown") || line.contains("Stopping worker pool synchronously...")) {
          if ("TEARDOWN".equals(opInProgress)) continue;
          if (opInProgress!=null) {
            ConcreteOperationInterval op = new ConcreteOperationInterval();
            op.begin = opStartTime;
            op.end = when;
            op.label = opInProgress;
            addDuration(op);
            job.sub.add(op);
            opInProgress = null;
          }
          opInProgress = "TEARDOWN";
          opStartTime = when;
        } else if (line.contains("Worker pool stopped")) {
          if (opInProgress=="TEARDOWN") {
            ConcreteOperationInterval op = new ConcreteOperationInterval();
            op.begin = opStartTime;
            op.end = when;
            op.label = "teardown";
            addDuration(op);
            job.sub.add(op);
            opInProgress = null;
          }
          job.end = when;
        }
      } catch (Exception x) {
        System.out.println("Parsing error: "+x.getMessage()+", continuing. The line was: "+line);
      }
    }
    if (null!=job && null!=job.begin && null!=job.end) {
      ret.add(job);
    }
    return ret;
  }

  private static void addDuration(ConcreteOperationInterval op) {
    long diff = (op.getEnd().getTime() - op.getStart().getTime())/1000;
    op.label += "\n" + diff+"s";
  }
}

