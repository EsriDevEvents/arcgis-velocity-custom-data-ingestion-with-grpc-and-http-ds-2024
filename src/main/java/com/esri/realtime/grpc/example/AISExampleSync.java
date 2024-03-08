package com.esri.realtime.grpc.example;

import com.esri.realtime.core.grpc.Feature;
import com.esri.realtime.core.grpc.GrpcFeedGrpc;
import com.esri.realtime.core.grpc.Request;
import com.esri.realtime.core.grpc.Response;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;

public class AISExampleSync {
  public static final String HOST_NAME               = "yourserver.com";
  public static final int    HOST_PORT               = 443; // MUST port 443
  public static final String GRPC_PATH_HEADER_KEY    = "grpc-path";
  public static final String GRPC_PATH_HEADER_VALUE  = "your-path-value-from-your-grpc-feed";

  public static void main(String[] args) throws InterruptedException {
    // Creating a communication channel opened between a client application and the Velocity gRPC feed.
    ManagedChannel channel = NettyChannelBuilder.forAddress(HOST_NAME, HOST_PORT).useTransportSecurity().build();
    // Creating metadata that contains header key-value pairs for the path value which is unique to the target feed
    // and possibly the token if ArcGIS authentication is used for the feed.
    Metadata metadata = new Metadata();
    Metadata.Key<String> grpcPathMetadataKey = Metadata.Key.of(GRPC_PATH_HEADER_KEY, Metadata.ASCII_STRING_MARSHALLER);
    metadata.put(grpcPathMetadataKey, GRPC_PATH_HEADER_VALUE);

    //Creating a blocking stub for synchronous send
    GrpcFeedGrpc.GrpcFeedBlockingStub blockingStub = GrpcFeedGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

    // Getting a sample AISData object, creating a request out of it.
    AISData ship = AISData.getOne();
    Request request = makeRequest(new Feature[] { AISData.asFeature(ship) });
    // Sending the request
    Response response = blockingStub.send(request);

    System.out.println("\tResponse: " + response.getCode() + " " + response.getMessage());
    if (channel != null) {
      channel.shutdown();
      channel = null;
    }
  }

  /**
   * Utility method that creates a Request from an array of features
   *
   * @param featureArray an array of features
   * @return a request that can be sent
   */
  static Request makeRequest(Feature[] featureArray)
  {
    final Request.Builder requestBuilder = Request.newBuilder();

    int numOfFeatures = featureArray.length;
    for (int i = 0; i < numOfFeatures; i++)
    {
      requestBuilder.addFeatures(featureArray[i]);
    }

    return requestBuilder.build();
  }
}
