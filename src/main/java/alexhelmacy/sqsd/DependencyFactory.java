
package alexhelmacy.sqsd;

import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

public class DependencyFactory {

    private DependencyFactory() {}

    /**
     * 
     * @param region A string representation of region
     * @return the aws region representation in the Java SDK V2.
     */
    public static final Region resolveRegion(String region){
        if (!(region instanceof String))throw new NullPointerException("Failed to resolve region. Region is null");
        Region resolvedRegion = null;//the resolved region
        switch(region){//switch based on the value if String param region
            case "af-south-1":
                resolvedRegion = Region.AF_SOUTH_1;
                break;
            case "ap-east-1":
                resolvedRegion = Region.AP_EAST_1;
                break;	
            case "ap-northeast-1":
                resolvedRegion = Region.AP_NORTHEAST_1;
                break;
            case "ap-northeast-2":
                resolvedRegion = Region.AP_NORTHEAST_2;
                break;
            case "ap-northeast-3":
                resolvedRegion = Region.AP_NORTHEAST_3;
                break;
            case "ap-south-1":
                resolvedRegion = Region.AP_SOUTH_1;
                break;
            case "ap-south-2":
                resolvedRegion = Region.AP_SOUTH_2;
                break;
            case "ap-southeast-1":
                resolvedRegion = Region.AP_SOUTHEAST_1;
                break;
            case "ap-southeast-2":
                resolvedRegion =Region.AP_SOUTHEAST_2;
                break;
            case "ap-southeast-3":
                resolvedRegion =Region.AP_SOUTHEAST_3;
                break;
            case "ap-southeast-4":
                resolvedRegion = Region.AP_SOUTHEAST_4;
                break;
            case "ap-southeast-5":
                resolvedRegion = Region.AP_SOUTHEAST_5;
                break;
            case "ca-central-1":
                resolvedRegion = Region.CA_CENTRAL_1;
                break;
            case "ca-west-1":
                resolvedRegion = Region.CA_WEST_1;
                break;
            case "cn-north-1":
                resolvedRegion = Region.CN_NORTH_1;
                break;
            case "cn-northwest-1":
                resolvedRegion = Region.CN_NORTHWEST_1;
                break;
            case "eu-central-1":
                resolvedRegion = Region.EU_CENTRAL_1;
                break;
            case "eu-central-2":
                resolvedRegion = Region.EU_CENTRAL_2;
                break;
            case "eu-north-1":
                resolvedRegion = Region.EU_NORTH_1;
                break;
            case "eu-south-1":
                resolvedRegion = Region.EU_SOUTH_1;
                break;
            case "eu-south-2":
                resolvedRegion = Region.EU_SOUTH_2;
                break;
            case "eu-west-1":
                resolvedRegion = Region.EU_WEST_1;
                break;
            case "eu-west-2":
                resolvedRegion = Region.EU_WEST_2;
                break;
            case "eu-west-3":
                resolvedRegion = Region.EU_WEST_3;
                break;
            case "il-central-1":
                resolvedRegion = Region.IL_CENTRAL_1;
                break;
            case "me-central-1":
                resolvedRegion = Region.ME_CENTRAL_1;
                break;
            case "me-south-1":
                resolvedRegion = Region.ME_SOUTH_1;
                break;
            case "sa-east-1":
                resolvedRegion = Region.SA_EAST_1;
                break;
            case "us-east-1":
                resolvedRegion = Region.US_EAST_1;
                break;
            case "us-east-2":
                resolvedRegion = Region.US_EAST_2;
                break;
            case "us-gov-east-1":
                resolvedRegion = Region.US_GOV_EAST_1;
                break;
            case "us-gov-west-1":
                resolvedRegion = Region.US_GOV_WEST_1;
                break;
            case "us-west-1":
                resolvedRegion = Region.US_WEST_1;
                break; 
            case "us-west-2":
                resolvedRegion = Region.US_WEST_2;
                break;
            default:
                throw new IllegalArgumentException("Unrecognized region: " + region);//region not recognized
        }
        return resolvedRegion;//return the resolved region
        
    }

    /**
     * 
     * @param region the aws region to create the SQS client
     * @return an sqs client for the specified region
     */
    public static SqsClient sqsClient(Region region) {
        return SqsClient.builder()
                       .httpClientBuilder(ApacheHttpClient.builder())
                       .region(region)
                       .build();
                       
    }

    /**
     * 
     * @param region the aws region as a string
     * @return an sqs client for the specified region after resolving the region to @param region.
     */
    public static SqsClient sqsClient(String region){
        return sqsClient(resolveRegion(region));
    }

    /**
     * 
     * @return an sqs client with region set to us-east-1
     */
    public static final SqsClient sqsClient(){
        return sqsClient(Region.US_EAST_1);
    }
}
