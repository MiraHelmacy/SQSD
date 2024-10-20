package alexhelmacy.sqsd;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import alexhelmacy.sqsd.processor.ExampleSqsDThread;
import alexhelmacy.sqsd.processor.SqsDThread;
import alexhelmacy.sqsd.processor.ExampleSqsDThread.ExampleSqsDThreadBuilder;

public class App {
    private static final CommandLineParser parser = new DefaultParser();//Command Parser
    private static final Options options = new Options();//Options
    private static final HelpFormatter formatter = new HelpFormatter();//Help Formatter

    private static boolean init = false;//has SQSD been started
    
    //SQSD attributes
    private static final String QUEUE_NAME_SHORT_PARAMETER = "q";
    private static final String QUEUE_NAME_LONG_PARAMETER = "queue-name";
    
    private static final String QUEUE_URL_SHORT_PARAMETER = "qu";
    private static final String QUEUE_URL_LONG_PARAMETER = "queue-url";

    private static final String QUEUE_ACCOUNT_ID_SHORT_PARAMETER = "aid";
    private static final String QUEUE_ACCOUNT_ID_LONG_PARAMETER = "account-id";

    private static final String THREAD_COUNT_SHORT_PARAMETER = "t";
    private static final String THREAD_COUNT_LONG_PARAMETER = "threads";

    private static final String MAX_MESSAGES_SHORT_PARAMETER = "m";
    private static final String MAX_MESSAGES_LONG_PARAMETER = "max-messages";

    private static final String WAIT_TIME_SHORT_PARAMETER = "w";
    private static final String WAIT_TIME_LONG_PARAMETER = "wait-time";

    private static final String MESSAGE_ATTRIBUTES_SHORT_PARAMETER = "ma";
    private static final String MESSAGE_ATTRIBUTES_LONG_PARAMETER = "message-attributes";

    private static final String SYSTEM_MESSAGE_ATTRIBUTES_SHORT_PARAMETER = "sa";
    private static final String SYSTEM_MESSAGE_ATTRIBUTES_LONG_PARAMETER = "system-attribute";

    private static final String HELP_MESSAGE_SHORT_PARAMETER = "h";
    private static final String HELP_MESSAGE_LONG_PARAMETER = "help";

    private static final String REGION_LONG_OPTION = "region";

    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static SqsD sqsd;

    /**
     * Initialize the options to be used in CLI. Should only be called once
     * @return if init was successful
     */
    synchronized static final boolean init(){
        if (!init){
            //add all options
            options.addOption(QUEUE_URL_SHORT_PARAMETER, QUEUE_URL_LONG_PARAMETER, true, "Queue URL to consume messages from. Required if Queue name is not present");
            options.addOption(QUEUE_NAME_SHORT_PARAMETER, QUEUE_NAME_LONG_PARAMETER, true, "Queue Name of SQS queue to consume messages from. Required if Queue URL is not present");
            options.addOption(MAX_MESSAGES_SHORT_PARAMETER, MAX_MESSAGES_LONG_PARAMETER, true, "Max Number of Messages to receive per request");
            options.addOption(WAIT_TIME_SHORT_PARAMETER, WAIT_TIME_LONG_PARAMETER, true, "How long to wait to receive messages");
            options.addOption(THREAD_COUNT_SHORT_PARAMETER, THREAD_COUNT_LONG_PARAMETER, true, "Number of threads. Max of 16");
            options.addOption(MESSAGE_ATTRIBUTES_SHORT_PARAMETER, MESSAGE_ATTRIBUTES_LONG_PARAMETER, true, "Message Attributes");
            options.addOption(SYSTEM_MESSAGE_ATTRIBUTES_SHORT_PARAMETER, SYSTEM_MESSAGE_ATTRIBUTES_LONG_PARAMETER, true, "System Parameters");
            options.addOption(HELP_MESSAGE_SHORT_PARAMETER, HELP_MESSAGE_LONG_PARAMETER, false, "Prints this help message");    
            options.addOption(QUEUE_ACCOUNT_ID_SHORT_PARAMETER, QUEUE_ACCOUNT_ID_LONG_PARAMETER, true, "AWS Account id of SQS Queue. Required if the Queue is in a different account.");        
            options.addOption(REGION_LONG_OPTION, REGION_LONG_OPTION, true, "AWS Region");
            //set list options
            options.getOption(MESSAGE_ATTRIBUTES_SHORT_PARAMETER).setArgs(Option.UNLIMITED_VALUES);
            options.getOption(SYSTEM_MESSAGE_ATTRIBUTES_LONG_PARAMETER).setArgs(Option.UNLIMITED_VALUES);
        }
        init = true;
        return init;
    }

    /**
     * Prints the help message 
     * @param footer String to add at the end of the help command. Can be used to displace exception information when starting this app.
     */
    synchronized private static final void help(String footer){
        String header = "Consume messages from an SQS queue";
        formatter.printHelp("sqsd", header, options, footer, true);
    }

    /**
     * default help method. No footer printed.
     */
    synchronized private static final void help(){
        help("");
    }

    /**
     * Stops the App.
     * @param reason why was the app stopped.
     */
    synchronized public static final void stop(String reason){
        if (sqsd instanceof SqsD){//sqsd is not null
            logger.info("Stopping SQSD: " + reason);//log that sqsd is stopping
            sqsd.stop(reason);//stop
        }else{//sqsd is null
            logger.warn("SQSD is null. ignoring.");//warn that sqsd is null.
        }
    }
    

    /**
     * requests the queue url based on the queue name if the queue url is not provided
     * @param queueName name of the queue
     * @param accountId AWS Account Id that owns the queue
     * @param region AWS region of the queue
     * @return the queue url
     */
    synchronized private static final String getQueueUrlFromQueueName(String queueName, String accountId, String region){
        GetQueueUrlRequest.Builder builder = GetQueueUrlRequest.builder()//GetQueueURlRequest Builder
                                                               .queueName(queueName);//add queue name
        if (accountId instanceof String)builder.queueOwnerAWSAccountId(accountId);//add account id if accound id is not null
        GetQueueUrlRequest request = builder.build();//build GetQueueUrlRequest
        SqsClient sqs = DependencyFactory.sqsClient(region);//Create an SQS Client with the region
        GetQueueUrlResponse response = sqs.getQueueUrl(request);//make the request of SQS
        return response.queueUrl();//return the queue url
    }
    /**
     * SQSD main function
     * @param args SQSD args
     * @throws Exception
     */
    synchronized public static void main(String... args) throws Exception {
        
        if (!init()){//init failed
            logger.error("Failed to init");//log the error
            System.exit(1);//exit application
        }

        String queueName = null;//queue name
        String queueUrl = null;//queue url
        String accountId = null;//account id
        int threadCount = 4;//default thread count
        int maxMessages = -1;//default max message
        int waitTime = -1;//default wait time
        String region = "us-east-1";//default region
        String[] messageAttributes = null;//default message attributes
        String[] systemAttributes = null;//default system attributes

        boolean exceptionEncountered = false;//boolean for if an exception has been 
        String exceptionMessage = "";//exception message
        try {
            CommandLine cmd = parser.parse(options, args);//parse the args
            if (cmd.hasOption(HELP_MESSAGE_SHORT_PARAMETER) || cmd.hasOption(HELP_MESSAGE_LONG_PARAMETER)){//help requested
                help();//print help message
                System.exit(0);//exit successfully
            }
            queueUrl = cmd.getOptionValue(QUEUE_URL_SHORT_PARAMETER);//get the queue url
            region = cmd.getOptionValue(REGION_LONG_OPTION, region);//get the region provided
            if (!(queueUrl instanceof String)){//if the queue url is null
                queueName = cmd.getOptionValue(QUEUE_NAME_SHORT_PARAMETER);//get the queue name
                if (!(queueName instanceof String)){//both queue name and queue url are null
                    throw new IllegalArgumentException("Queue Name or Queue URL must be specified.");
                }
                accountId = cmd.getOptionValue(QUEUE_ACCOUNT_ID_SHORT_PARAMETER);//get the account id
                queueUrl = getQueueUrlFromQueueName(queueName, accountId, region);//get the queue url based on the queue name
            }

            threadCount = Integer.parseInt(cmd.getOptionValue(THREAD_COUNT_SHORT_PARAMETER, Integer.toString(threadCount)));//get the requested thread count
            maxMessages = Integer.parseInt(cmd.getOptionValue(MAX_MESSAGES_SHORT_PARAMETER, Integer.toString(maxMessages)));//get max messages
            waitTime = Integer.parseInt(cmd.getOptionValue(WAIT_TIME_SHORT_PARAMETER, Integer.toString(waitTime)));//get the wait time for the CLI
            
            messageAttributes = cmd.getOptionValues(MESSAGE_ATTRIBUTES_SHORT_PARAMETER);//get the message attributes
            systemAttributes = cmd.getOptionValues(SYSTEM_MESSAGE_ATTRIBUTES_SHORT_PARAMETER);//get the system attributes
            
        } catch (ParseException pe){//cmd failed to parse
            exceptionMessage = "Failed to parse args: " + pe.getMessage();//get failure message
            exceptionEncountered = true;//an exception was encountered
        } catch (SqsException sqse){//an sqs exception
            exceptionMessage = "Failed to get queue url: " + sqse.getMessage();//failed to get the url
            exceptionEncountered = true;//exception encountered
        } catch (NumberFormatException nfe){//non number input
            exceptionMessage = "Failed to parse the arguments: " + nfe.getMessage();//get the message
            exceptionEncountered = true;//exception encountered
        } catch (Exception e){//a generic exception encountered
            exceptionMessage = "Some Other Exception occurred: " + e.getMessage();//get the exception message
            exceptionEncountered = true;//exception encountered
        } finally {
            if (exceptionEncountered){//an exception was encountered
                help(exceptionMessage);//print the help with the exception that occurred
                System.exit(1);//exit
            }
        }

        if (threadCount <= 0){
            logger.warn("Thread count specified is less than or equal to 0. Setting to default of 4.");
            threadCount = 4;
        }
        
        List<SqsDThread> threads = new ArrayList<>();//list of threads
        ExampleSqsDThreadBuilder builder = ExampleSqsDThread.builder();//example sqsd thread builder

        builder.queueUrl(queueUrl)//add the queue url
               .region(region);//add region
        if (maxMessages > 0)builder.maxNumberOfMessages(maxMessages);//add max messages if present
        if (waitTime >= 0)builder.waitTime(waitTime);//add wait time if present
        if (messageAttributes != null)builder.messageAttributes(Arrays.asList(messageAttributes));//add message attributes if present
        if (systemAttributes != null)builder.systemAttributes(Arrays.asList(systemAttributes));//add system attributes if present
        
        
        
        for (int i = 0; i < threadCount; i++)threads.add(new ExampleSqsDThread(builder));//add as many threads as requested
        
        sqsd = new SqsD(threads);//create an instance of sqsd with the threads list
        sqsd.start();//start sqsd

        //add shutdown hook to stop sqsd
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                App.stop("Application Stop Requested.");//stop the app
            }
        }));
    }
}
