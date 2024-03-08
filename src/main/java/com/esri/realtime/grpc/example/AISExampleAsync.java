package com.esri.realtime.grpc.example;

import com.esri.realtime.core.grpc.Feature;
import com.esri.realtime.core.grpc.GrpcFeedGrpc;
import com.esri.realtime.core.grpc.GrpcFeedGrpc.GrpcFeedStub;
import com.esri.realtime.core.grpc.Request;
import com.esri.realtime.core.grpc.Response;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class AISExampleAsync {

  // Connection parameters
  public static final String HOST_NAME               = "yourserver.com";
  public static final int    HOST_PORT               = 443;  // MUST port 443
  public static final String GRPC_PATH_HEADER_KEY    = "grpc-path";
  public static final String GRPC_PATH_HEADER_VALUE  = "your-path-value-from-your-grpc-feed";

  // Interval, volume and duration information related to sending data
  public static final int SEND_INTERVAL_MS = 1000;
  public static final int PER_SEND_SIZE = 2000;
  public static final int MAX_RUN_TIME_MS = 10000;

  // Non-static variables
  private ManagedChannel channel;
  private GrpcFeedStub asyncStub;
  private StreamObserver<Request> streamClientSender;
  private StreamObserver<Response> responseStreamObserver;

  public AISExampleAsync() {
    // Creating a communication channel opened between a client application and the Velocity gRPC feed.
    channel = NettyChannelBuilder.forAddress(HOST_NAME, HOST_PORT).useTransportSecurity().build();
    // Creating metadata that contains header key-value pairs for the path value which is unique to the target feed
    // and possibly the token if ArcGIS authentication is used for the feed.
    Metadata metadata = new Metadata();
    Metadata.Key<String> grpcPathMetadataKey = Metadata.Key.of(GRPC_PATH_HEADER_KEY, Metadata.ASCII_STRING_MARSHALLER);
    metadata.put(grpcPathMetadataKey, GRPC_PATH_HEADER_VALUE);

    // Creating a non-blocking stub for streaming
    asyncStub = GrpcFeedGrpc.newStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    // Creating a StreamObserver for Response which is needed by the stream method
    responseStreamObserver = getServerResponseObserver();
  }

  /**
   * Creates a StreamObserver for Response.  Implement the callback methods.
   * @return a StreamObserver for Response
   */
  public StreamObserver<Response> getServerResponseObserver(){
    StreamObserver<Response> observer = new StreamObserver<Response>(){
      @Override
      public void onNext(Response response) {
        if (response.getCode() != 200)
          System.out.println("\tFailure response: " + response.getCode() + " " + response.getMessage());
        else
          System.out.println("\tSuccess response: " + response.getCode() + " " + response.getMessage());
      }

      @Override
      public void onError(Throwable throwable) {
        System.out.println("Error occurred while streaming features" + throwable);
      }

      @Override
      public void onCompleted() {
        System.out.println("On complete called.");
      }
    };
    return observer;
  }

  /**
   * Uses the streamClientSender to stream requests to the gRPC server
   *
   * @param ships an Array of AISData objects
   * @throws InterruptedException
   */
  public void sendMessage(AISData[] ships) throws InterruptedException
  {
    System.out.println("Sending data...");
    if(streamClientSender == null) {
      streamClientSender = asyncStub.stream(responseStreamObserver);
    }
    try {
      List<Feature> featureList = new ArrayList<Feature>();
      for (AISData ship : ships)
      {
        featureList.add(AISData.asFeature(ship));
      }
      Request request = makeRequest(featureList.toArray(new Feature[featureList.size()]));
      streamClientSender.onNext(request);
    }
    catch (StatusRuntimeException e) {
      System.out.println("Error occurred in SendMessage" + e);
    }
  }

  /**
   * Stops the stream and shuts down the channel
   */
  public void stopStream() {
    if (streamClientSender != null) {
      streamClientSender.onCompleted();
      streamClientSender = null;
    }
    if (channel != null) {
      channel.shutdown();
      channel = null;
    }
  }

  public static void main(String[] args) throws InterruptedException {

    // Creating an object of this class because non-static variables are used.
    AISExampleAsync example = new AISExampleAsync();
    AISFileReader reader = new AISFileReader();

    try {
      Timer timer = new Timer();
      long startTime = System.currentTimeMillis();
      // Schedule a new TimerTask
      timer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          try {
            // Send an array of AISData objects
            example.sendMessage(reader.getData(PER_SEND_SIZE));
          } catch (Exception e) {
            e.printStackTrace();
          }

          // Check the time it has been running.  Close down if exceeds MAX_RUN_TIME_MS.
          if (System.currentTimeMillis() - startTime >= MAX_RUN_TIME_MS) {
            example.stopStream();
            if (reader != null)
              reader.close();

            // Get a chance to observe the response when stream is closed
            try {
              Thread.sleep(5000);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            System.out.println("Done.");
            timer.cancel();
          }
        }
      }, 100, SEND_INTERVAL_MS);
    } catch (Exception e) {
      e.printStackTrace();
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
