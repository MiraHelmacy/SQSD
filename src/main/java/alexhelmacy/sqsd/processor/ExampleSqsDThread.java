package alexhelmacy.sqsd.processor;


import java.util.*;

import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.Set;

import java.security.InvalidParameterException;
import java.util.ArrayList;

public class ExampleSqsDThread extends SqsDThread{

  /**
   * return an sqsd thread builder
   * @return a new instance of an ExampleSqsDThreadBuilder
   */
  public static final ExampleSqsDThreadBuilder builder(){//static SQSD Thread builder
    return new ExampleSqsDThreadBuilder();
  }

  /**
   * Example SQSD thread builder class
   */
  public static final class ExampleSqsDThreadBuilder{
    private String queueUrl = null;//queue url
    private String region = "us-east-1";//region
    private int maxNumberOfMessages = 10;//default number of messages
    private int waitTime = 20;//wait time
    private Collection<String> messageAttributes = Arrays.asList(new String[]{"All"});//message attributes
    private Collection<String> systemAttributes = Arrays.asList(new String[]{"All"});//system message attributes

    /**
     * sets the queue url
     * @param queueUrl the queue url
     * @return itself
     */
    public final ExampleSqsDThreadBuilder queueUrl(String queueUrl){
      this.queueUrl = queueUrl;
      return this;
    }

    /**
     * sets the regions
     * @param region the region as a string
     * @return itself
     */
    public final ExampleSqsDThreadBuilder region(String region){
      this.region = region;
      return this;
    }

    /**
     * sets max messages
     * @param maxMessages the maximum number of messages
     * @return itself
     */
    public final ExampleSqsDThreadBuilder maxNumberOfMessages(int maxMessages){
      maxNumberOfMessages = maxMessages;
      return this;
    }

    /**
     * sets the wait time
     * @param waitTime the wait time in seconds 
     * @return itself
     */
    public final ExampleSqsDThreadBuilder waitTime(int waitTime){
      this.waitTime = waitTime;
      return this;
    }

    /**
     * sets the message attributes
     * @param messageAttributes the message attributes
     * @return itself
     */
    public final ExampleSqsDThreadBuilder messageAttributes(Collection<String> messageAttributes){
      this.messageAttributes = messageAttributes;
      return this;
    }

    /**
     * sets the system attributes
     * @param systemAttributes the system message attributes
     * @return itself
     */
    public final ExampleSqsDThreadBuilder systemAttributes(Collection<String> systemAttributes){
      this.systemAttributes = systemAttributes;
      return this;
    }

    /**
     * builds an instance of the Example SQSD thread data
     * @return an instance of ExampleSqsDThreadData
     */
    public final ExampleSqsDThreadData build(){
      return new ExampleSqsDThreadData(queueUrl, region, maxNumberOfMessages, waitTime, messageAttributes, systemAttributes);
    }
  }

  /**
   * ExampleSqsDThreadData class
   */
  public static final class ExampleSqsDThreadData{
    public final String QUEUE_URL;//the queue url
    public final String REGION;//the region
    public final int MAX_NUMBER_OF_MESSAGES;//the maximum number of messages received
    public final int WAIT_TIME_SECONDS;//the wait time in seconds
    public final Collection<String> MESSAGE_ATTRIBUTE_NAMES;//the message attributes
    public final Collection<String> MESSAGE_SYSTEM_ATTRIBUTE_NAMES;//the system attributes

    /**
     * Constructor
     * @param queueUrl the queue url
     * @param region the aws region
     * @param maxMessages the max messages
     * @param waitTime the wait time in seconds
     * @param messageAttributes the message attributes
     * @param systemAttributes the system attributes
     */
    ExampleSqsDThreadData(String queueUrl, String region, int maxMessages, int waitTime, Collection<String> messageAttributes, Collection<String> systemAttributes){
      QUEUE_URL = queueUrl;
      REGION = region;
      MAX_NUMBER_OF_MESSAGES = maxMessages;
      WAIT_TIME_SECONDS = waitTime;
      MESSAGE_ATTRIBUTE_NAMES = messageAttributes;
      MESSAGE_SYSTEM_ATTRIBUTE_NAMES = systemAttributes;
    }

    /**
     * Construstor without message and system attributes. Implicitly calls the constructor with all of the parameters
     * @param queueUrl the queue url
     * @param region the aws region
     * @param maxMessages the max number of messages
     * @param waitTime the wait time in seconds
     */
    ExampleSqsDThreadData(String queueUrl, String region, int maxMessages, int waitTime){
      this(queueUrl, region, maxMessages, waitTime, new ArrayList<>(), new ArrayList<>());
    }

    /**
     * Constructor with just the region and queue url. Implicitly calls constructor with queue url, region, max messages and wait time
     * @param queueUrl the queue url
     * @param region the aws region
     */
    ExampleSqsDThreadData(String queueUrl, String region){
      this(queueUrl, region, 10, 20);
    }
  }
  
  private final int maxNumberOfMessages;//the maximum number of messages
  private final int waitTimeSeconds;//the wait time in seconds
  private final Collection<String> messageAttributeNames;//the message attributes
  private final Collection<String> messageSystemAttributeNames;//the system attributes

  /**
   * Constructor of an example sqsd thread
   * @param queueUrl the queue url
   * @param region the aws region
   * @param maxMessages the maximum number of messages
   * @param waitTime the wait time in seconds
   * @param messageAttributeNames the message attributes
   * @param messageSystemAttributeNames the system attributes
   */
  public ExampleSqsDThread(String queueUrl, String region,int maxMessages, int waitTime, Collection<String> messageAttributeNames, Collection<String> messageSystemAttributeNames){
    super(queueUrl, region);//queue url and region are in SqsDThread
    maxNumberOfMessages = maxMessages;
    waitTimeSeconds = waitTime;
    this.messageAttributeNames = messageAttributeNames;
    this.messageSystemAttributeNames = messageAttributeNames;
  }

  /**
   * Constructor with ExampleSqsDThreadData callse constructor with all parameters
   * @param data the data used to make the sqsd thread
   */
  public ExampleSqsDThread(ExampleSqsDThreadData data){
    this(data.QUEUE_URL, data.REGION, data.MAX_NUMBER_OF_MESSAGES, data.WAIT_TIME_SECONDS, data.MESSAGE_ATTRIBUTE_NAMES, data.MESSAGE_SYSTEM_ATTRIBUTE_NAMES);
  }

  /**
   * Constructor with sqsd thread builder. creates thread with built data
   * @param builder a builder for the sqsd thread
   */
  public ExampleSqsDThread(ExampleSqsDThreadBuilder builder){
    this(builder.build());
  }
  
  /**
   * Constructor with queue url
   * @param queueUrl
   */
  public ExampleSqsDThread(String queueUrl){
    this(ExampleSqsDThread.builder().queueUrl(queueUrl));
  }

  /**
   * 
   * @return max number of messages
   */
  synchronized private final int maxNumberOfMessages(){
    return maxNumberOfMessages;
  }

  /**
   * 
   * @return wait time in seconds
   */
  synchronized private final int waitTimeSeconds(){
    return waitTimeSeconds;
  }

  /**
   * 
   * @return the message attributes
   */
  synchronized private final Collection<String> messageAttributeNames(){
    return messageAttributeNames;
  }

  /**
   * 
   * @return system attributes
   */
  synchronized private final Collection<String> messageSystemAttributeNames(){
    return messageSystemAttributeNames;
  }

  /**
   * implementation of defaultRequest
   * @return the default ReceiveMessageRequest
   */
  @Override
  synchronized protected ReceiveMessageRequest defaultRequest() {
    return ReceiveMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .maxNumberOfMessages(maxNumberOfMessages())
                                .waitTimeSeconds(waitTimeSeconds())
                                .messageAttributeNames(messageAttributeNames())
                                .messageSystemAttributeNamesWithStrings(messageSystemAttributeNames())
                                .build();
  }

  /**
   * Implementation of receiveMessages
   * @param request the ReceiveMessageRequest
   * @return the ReceiveMessageResponse for the request
   * @throws InterruptedException if the thread is interrupted or the thread is closed
   */
  @Override
  synchronized protected ReceiveMessageResponse receiveMessages(ReceiveMessageRequest request) throws InterruptedException {
    if (Thread.currentThread().isInterrupted())throw new InterruptedException("Thread Interrupted");
    if (closed()) throw new InterruptedException("sqs client closed");
    return sqs.receiveMessage(request);
  }

  /**
   * process the messages. In this example, just print the message
   * @param response the ReceiveMessageResponse from receiveMessages
   * @return a list of processed messages
   * @throws InterruptedException if the thread is interrupted or the thread is closed.
   */
  @Override
  synchronized protected List<Message> processMessages(ReceiveMessageResponse response) throws InterruptedException {
    if (Thread.currentThread().isInterrupted())throw new InterruptedException("Thread Interrupted");
    if (closed()) throw new InterruptedException("thread should stop");
    List<Message> processedMessages = new ArrayList<>();
    if (response.hasMessages()){
      response.messages()
              .stream()
              .forEach((message) -> {
                logger.info("Receive Message: " + message);
                processedMessages.add(message);
              });
    }

    return processedMessages;
  }

  /**
   * delete the messages from the queue.
   * @param messages the list of messages to delete
   * @return the DeleteMessageBatchResponse.
   * @throws InterruptedException if the thread is interrupted or closed.
   */
  @Override
  synchronized protected DeleteMessageBatchResponse deleteMessages(List<Message> messages) throws InterruptedException {
    if (Thread.currentThread().isInterrupted())throw new InterruptedException("Thread Interrupted");
    if (closed()) throw new InterruptedException("sqs client closed");
    List<DeleteMessageBatchRequestEntry> entries = messages.stream()
                                                           .map((message) -> {
                                                            return DeleteMessageBatchRequestEntry.builder()
                                                                                          .receiptHandle(message.receiptHandle())
                                                                                          .id(message.messageId())
                                                                                          .build();
                                                           })
                                                           .collect(Collectors.toList());
    
    DeleteMessageBatchRequest deleteMessageBatchRequest = DeleteMessageBatchRequest.builder()
                                                                                   .entries(entries)
                                                                                   .queueUrl(queueUrl)
                                                                                   .build();
    return sqs.deleteMessageBatch(deleteMessageBatchRequest);
  }

  /**
   * stop the sqsd thread.
   */
  @Override
  synchronized public final void stop() {
    running = false;
    closed = true;
    sqs.close();
  }

  /**
   * back off method in case of failure
   * @param retry the current retry represented by a long
   * @param maxBackoffMillis the maximum back of in milliseconds. Prevents the thread from stopping for eternity
   * @throws InterruptedException when thread is interrupted.
   */
  synchronized protected final void backoff(long retry, long maxBackoffMillis) throws InterruptedException{
    long backoffMillis = Math.min(retry * 50, maxBackoffMillis);
    if (backoffMillis < 0){
      logger.warn("Back off millis overflowed");
      backoffMillis = maxBackoffMillis;
    }
    logger.info("Backoff: " + backoffMillis);
    Thread.sleep(retry * 50);
  }

  /**
   * work horse of the thread class. After some checks on if the thread is already running. loop infinitely until interrupted. 
   * Application does 4 things.
   * 1. Receive messages from an SQS Queue.
   * 2. Processes the received messages.
   * 3. Deletes the processed messages.
   * 4. Retries failed deleted messages.
   */
  @Override
  public void run(){
    synchronized(this){
      logger.info("Run Method called");
      if (closed()){ //check if the thread has been stopped
        logger.error("Thread Already Stopped");
        throw new InvalidParameterException("SqsD thread closed");
      }
      if (running()){//check if the thread is already running. return if so.
        logger.warn("Already Running");
        return;
      }
      if (!running()) {//if not running, set running to true. should only occur once
        running = true;
      }
  
      logger.info("Starting Thread");    
    }
    
    while (running()){//while running
        try{
            synchronized(logger){
              logger.info("Receiving messages");  
            }
            
            //receive messages
            ReceiveMessageResponse receiveMessageResponse;
            synchronized(this){
              receiveMessageResponse = receiveMessages();
            }
            
            //check if there are messages in the response
            if (receiveMessageResponse.hasMessages()){
              synchronized(logger){
                logger.debug("Received Messages: " + receiveMessageResponse);
                logger.info("Processing Messages");
              }
              
              //process messages
              List<Message> processedMessages;
              synchronized (this){
                processedMessages = processMessages(receiveMessageResponse);
              }

              synchronized(logger){
                logger.debug("Processed messages: " + processedMessages);
                logger.info("Deleting messages");
              }
              //delete messages
              DeleteMessageBatchResponse deleteMessageBatchResponse;
              synchronized(this){
                deleteMessageBatchResponse = deleteMessages(processedMessages);
              }
              synchronized(logger){
                logger.debug("Delete Message Response: " + deleteMessageBatchResponse);  
              }
              //handle failed deleted messages
              List<DeleteMessageBatchResponse> deleteMessageBatchResponses = new ArrayList<>();
              deleteMessageBatchResponses.add(deleteMessageBatchResponse);
              long retry = 0;

              while (deleteMessageBatchResponse.hasFailed()){//while there are failed messages
                Set<String> failedIds = new HashSet<>();//hashset of failed ids
                synchronized(logger){
                  deleteMessageBatchResponse.failed()//failed entries
                                            .stream()//stream
                                            .forEach((batchResultErrorEntry) -> {//for each failed entry
                                              logger.debug("Failed Batch Entry: " + batchResultErrorEntry);
                                              failedIds.add(batchResultErrorEntry.id());//add the failed message ids
                                            });
                }
                
                
                if (retry >= 10)break;//stop if we have retried 10 times or more
                backoff(retry, 500);//back off
                
                //create list of messages that failed to be deleted
                List<Message> failedMessages = processedMessages.stream()
                                                                .filter((message) -> failedIds.contains(message.messageId()))
                                                                .collect(Collectors.toList());
                synchronized(logger){
                  logger.info("Deleting failed messages");
                }
                
                synchronized(this){
                  deleteMessageBatchResponse = deleteFailedMessages(failedMessages);//delete failed messages
                }

                synchronized(logger){
                  logger.debug("Delete Message Response: " + deleteMessageBatchResponse);
                }
                
                deleteMessageBatchResponses.add(deleteMessageBatchResponse);//add the response
                retry++;//increment retry
              }
              //log messages that were deleted
              synchronized(logger){
                deleteMessageBatchResponses.stream()
                                           .filter((messageBatchResponse) -> messageBatchResponse.hasSuccessful())//get only successful responses
                                           .map((successfulDeleteMessageBatchResponse) -> successfulDeleteMessageBatchResponse.successful())//List<List<DeleteMessageBatchResultEntry>>
                                           .flatMap(List::stream)//flatten
                                           .collect(Collectors.toList())//collect to list
                                           .forEach((successfulDeleteMessageBatchResultEntry) -> {//for each DeleteMessageBatchResultEntry
                                             logger.info("Messages Processed: " + successfulDeleteMessageBatchResultEntry);
                                           });
              }            
          }
        }catch (InterruptedException ie){//thread was interrupted
          synchronized(logger){
            logger.info("Thread Interrupted: " + ie.getMessage());
          }
          stop();//stop
        }
    }
    logger.info("Thread Stopped!");
  }
}
