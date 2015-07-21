package com.google.bunnylog.analyzer;

import com.google.bunnylog.analyzer.core.Log4jParser;
import com.google.bunnylog.analyzer.core.OperationInterval;
import com.google.bunnylog.analyzer.core.Render;
import com.google.bunnylog.analyzer.core.Summarize;
import com.google.bunnylog.analyzer.gcp.ClientStdoutParser;
import com.google.bunnylog.analyzer.gcp.DataflowParser;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Summarizes and renders the Dataflow log given as argument.
 * <p/>
 * (logsummarizer can be used as a library, but this gives a simple command-line access)
 */
public class Main {

  public static void help() {
    System.out.println("Usage: Main [-l] <logfile> [-j <jobId>]");
    System.out.println(" -l indicates Log4j format; otherwise GCP format is assumed.");
    System.out.println(" if you specify a jobId, then other jobs are filtered out of the results.");
  }

  public static void main(String[] args) throws Exception {
    String dfLog = null;
    ArrayList<String> l4jLogs = new ArrayList<>();
    String jobId = null;
    if (args.length < 1) {
      help();
      return;
    }
    for (int i=0; i<args.length; i++) {
      String a = args[i];
      if (a.equalsIgnoreCase("--help")) {
        help();
        return;
      }
      if (a.equals("-l")) {
        l4jLogs.add(args[++i]);
      } else if (a.equals("-d")) {
        dfLog = args[++i];
      } else if (a.equals("-j")) {
        jobId = args[++i];
      } else {
        // this must be the Dataflow log file
        dfLog = a;
      }
    }

    if (null==dfLog && null==l4jLogs) {
      help();
      return;
    }

    ArrayList<OperationInterval> ops = new ArrayList<>();
    if (null!=dfLog) ops.addAll(DataflowParser.parse(dfLog, jobId));
    for (String l : l4jLogs) {
      ops.addAll(Log4jParser.parse(l));
      ops.addAll(ClientStdoutParser.parse(l));
    }
    if (null != jobId) {
      System.out.println("Summary for Job ID " + jobId);
    } else {
      System.out.println("Summary");
    }
    new Summarize(ops).printTable();
    new Render(ops).toSvg(new PrintStream(new File("out.svg")));
    System.out.println("Graph saved to 'out.svg'");
    // Web browsers can open svg files.
  }

}
