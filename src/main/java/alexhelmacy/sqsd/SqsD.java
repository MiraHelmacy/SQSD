package alexhelmacy.sqsd;


import alexhelmacy.sqsd.processor.ExampleSqsDThread;
import alexhelmacy.sqsd.processor.SqsDThread;

import java.lang.IllegalArgumentException;

import java.util.List;
import java.util.ArrayList;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SqsD implements ISqsD{
  private final ExecutorService SERVICE;//an executor service
  private final List<SqsDThread> THREADS;//a list of threads
  private static final int MAX_THREADS = 16;//the maximum number of threads SQSD can handle
  private final Logger LOGGER = LoggerFactory.getLogger(this.getClass().getSimpleName());//logger for logging information


  /**
   * return a default list if threads
   * @param queueUrl the queue url to get messages from
   * @param numThreads how many threads to create
   * @return an array list of SqsD threads
   */
  synchronized public static final List<SqsDThread> defaultThreadsList(String queueUrl, int numThreads){
    List<SqsDThread> defaultThreads = new ArrayList<>();
    if (numThreads > MAX_THREADS)numThreads = MAX_THREADS;
    for (int i = 0; i < numThreads; i++){
      defaultThreads.add(new ExampleSqsDThread(queueUrl));
    }
    return defaultThreads;
  }

  /**
   * Sqsd constructor
   * @param queueUrl the queue url
   * @param numThreads how many threads
   */
  public SqsD(String queueUrl, int numThreads){
    this(defaultThreadsList(queueUrl, numThreads));
  }

  /**
   * Sqsd constructor
   * @param threads a list of SqsDThreads
   */
  public SqsD(List<SqsDThread> threads){
    this.THREADS = threads;//get the threads
    if (this.THREADS.size() > MAX_THREADS)throw new IllegalArgumentException("Too Many Threads");//too many threads check
    if (this.THREADS.isEmpty())throw new IllegalArgumentException("Threads must be at least 0");//list is empty
    
    //create fixed thread pool with threads double the size of the number of threads. 
    //Fixed thread pool is double the number of threads to enable stopping each thread concurrently at shutdown.
    this.SERVICE = Executors.newFixedThreadPool(this.THREADS.size() * 2);
    
  }

  /**
   * Start SQSD
   */
  @Override
  synchronized public void start() {
    LOGGER.info("SQSD STARTED");//log started
    for (SqsDThread thread: THREADS){//for each thread
      SERVICE.submit(thread);//submit the thread to the SERVICE
    }
  }

  /**
   * stop the service
   * @param reason the reason for stopping the service
   */
  @Override
  synchronized public void stop(String reason){
    LOGGER.info("Stopping: " + reason);//log the reason for stopping
    try{
      LOGGER.info("Stopping Threads");//log stopping all threads
      for (SqsDThread thread: THREADS){//for each thread in THREADS
        //submit a new Runnable to stop the thread
        SERVICE.submit(new Runnable() {
          @Override
          public void run() {
            LOGGER.info("Stopping Thread - " + thread.THREAD_ID);//log that the thread is requested to stop
            thread.stop();//stop the thread
          }
        });
      }
      LOGGER.info("Attempting service shutdown");//log attempted shutdown
      SERVICE.shutdown();//shutdown the service
      LOGGER.info("Awaiting Termination");//log awaiting termination
      if (!SERVICE.awaitTermination(30, TimeUnit.SECONDS)){//awaiting 30 seconds for service to terminate failed
        LOGGER.info("Forcing Termination");//log forcing termination
        SERVICE.shutdownNow();//force termination
      }
      
    }catch(InterruptedException ie){
      LOGGER.info("Threads Interrupted. Forcing Termination.", ie);//log thread interrupted
      SERVICE.shutdownNow();//stop the service
    }finally{
      LOGGER.info("SQSD STOPPED");//log SQSD has stopped
    }
  }
}
