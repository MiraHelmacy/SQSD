package alexhelmacy.sqsd.processor;


import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alexhelmacy.sqsd.DependencyFactory;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

public abstract class SqsDThread implements ISqsDThread{
  public final String THREAD_ID = UUID.randomUUID().toString();//thread id
  
  protected boolean running = false;//is the thread running?
  protected boolean closed = false;//is the thread closed?
  
  protected final Logger logger;//sqsd thread logger
  protected final SqsClient sqs;//sqs client for the sqsd thread

  protected final String queueUrl;//the queue url

  /**
   * SqsD Thread Constructor
   * @param queueUrl the queue url to read messages from
   * @param region the region of the sqs queue
   */
  public SqsDThread(String queueUrl, String region){
    this.logger = LoggerFactory.getLogger(this.getClass().getSimpleName() + "-" + THREAD_ID);//assign a logger with the thread if id
    sqs = DependencyFactory.sqsClient(region);//create the sqsd client
    this.queueUrl = queueUrl;//assign the queue url
  }

  //abstract methods for basic SQSD thread operations.
  abstract protected ReceiveMessageRequest defaultRequest();//default request used to delete the messages
  abstract protected ReceiveMessageResponse receiveMessages(ReceiveMessageRequest request) throws InterruptedException;//receive message
  abstract protected List<Message> processMessages(ReceiveMessageResponse response) throws InterruptedException;//process messages
  abstract protected DeleteMessageBatchResponse deleteMessages(List<Message> messages) throws InterruptedException;//delete messages

  /**
   * delete messages that failed to be processed.
   * @param failedMessages the list of failed messages
   * @return the response of deleteMessages
   * @throws InterruptedException if thread is interrupted or the thread is closed
   */
  synchronized protected DeleteMessageBatchResponse deleteFailedMessages(List<Message> failedMessages) throws InterruptedException{//delete failed messages implementation
    if (Thread.currentThread().isInterrupted())throw new InterruptedException("Thread Interrupted");
    if (closed()) throw new InterruptedException("Thread Should close");
    return deleteMessages(failedMessages);
  }
  /**
   * receive messages from SQS Queue. implicitly calls the default request method to be implemented 
   * @return the response from receiving the messages from the SQS queue
   * @throws InterruptedException if the thread is interrupted or the thread is closed
   */
  synchronized protected ReceiveMessageResponse receiveMessages() throws InterruptedException{//receive messages with default request
    if (Thread.currentThread().isInterrupted())throw new InterruptedException("Thread Interrupted");
    if (closed()) throw new InterruptedException("Thread Should close");
    return receiveMessages(defaultRequest());
  }  

  /**
   * @return if the thread is running
   */
  synchronized public final boolean running(){//check if thread is running
    return running;
  }

  /**
   * @return if thread is closed.
   */
  synchronized public final boolean closed(){//check if thread is closed
    return closed;
  }
}
