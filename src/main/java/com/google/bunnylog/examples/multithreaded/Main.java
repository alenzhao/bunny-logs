package com.google.bunnylog.examples.multithreaded;

import com.google.bunnylog.logger.BunnyLog;

import org.apache.logging.log4j.LogManager;


/**
 * A sample multithreaded program that generates a log that we can then process.
 *
 * You can run this example via the following command:
 * mvn exec:java -Dexec.mainClass="com.google.bunnylog.examples.multithreaded.Main"
 */
public class Main {
  BunnyLog bl;

  public static void main(String[] args) throws InterruptedException {
    new Main().startJob();
  }

  // A simple step. It'll show up as an oval inside
  // of the "main" task rectangle.
  public void growCarrots() throws InterruptedException {
    Thread.sleep(5000);
    bl.stepEnd("growCarrots");
  }

  // spawns worker threads. Each of these threads
  // will show up as a rectangle in the visualization.
  public void harvest() throws InterruptedException {
    Thread[] workers = new Thread[4];
    for (int i=0; i<4; i++) {
      workers[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          BunnyLog log = new BunnyLog(LogManager.getLogger());
          log.start("harvest");
          try {
            Thread.sleep(5000);
          } catch (InterruptedException x) {
            // ignore
          }
          log.end();
        }
      });
      workers[i].start();
    }
    for (int i=0; i<4; i++) {
      workers[i].join();
    }
    bl.stepEnd("oversee harvest threads");
  }

  // Another simple step.
  public void feedRabbit() throws InterruptedException {
    Thread.sleep(5000);
    bl.stepEnd("feedRabbit");
  }

  public void startJob() throws InterruptedException {
    bl = new BunnyLog(LogManager.getLogger());
    bl.start("main");
    growCarrots();
    harvest();
    feedRabbit();
    bl.end();
    Thread.sleep(100);
    System.out.println("We're done! Next: ");
    System.out.println("  1) copy this output to log.txt (if you haven't already)");
    System.out.println("  2) run mvn exec:java -Dexec.mainClass=\"com.google.bunnylog.analyzer.Main\" -Dexec.args=\"-l log.txt\"");
    System.out.println("  3) open out.svg in your web browser (or svg editor if you have one).");
  }

}
