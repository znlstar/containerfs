package dp;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.0)",
    comments = "Source: datanode.proto")
public class DataNodeGrpc {

  private DataNodeGrpc() {}

  public static final String SERVICE_NAME = "dp.DataNode";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<dp.Datanode.WriteChunkReq,
      dp.Datanode.WriteChunkAck> METHOD_WRITE_CHUNK =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "dp.DataNode", "WriteChunk"),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.WriteChunkReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.WriteChunkAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<dp.Datanode.StreamReadChunkReq,
      dp.Datanode.StreamReadChunkAck> METHOD_STREAM_READ_CHUNK =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "dp.DataNode", "StreamReadChunk"),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.StreamReadChunkReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.StreamReadChunkAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<dp.Datanode.DeleteChunkReq,
      dp.Datanode.DeleteChunkAck> METHOD_DELETE_CHUNK =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "dp.DataNode", "DeleteChunk"),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.DeleteChunkReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.DeleteChunkAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<dp.Datanode.DatanodeHealthCheckReq,
      dp.Datanode.DatanodeHealthCheckAck> METHOD_DATANODE_HEALTH_CHECK =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "dp.DataNode", "DatanodeHealthCheck"),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.DatanodeHealthCheckReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.DatanodeHealthCheckAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<dp.Datanode.RecvMigrateReq,
      dp.Datanode.RecvMigrateAck> METHOD_RECV_MIGRATE_MSG =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "dp.DataNode", "RecvMigrateMsg"),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.RecvMigrateReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.RecvMigrateAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<dp.Datanode.FInfo,
      dp.Datanode.SendAck> METHOD_SEND_MIGRATE_DATA =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING,
          generateFullMethodName(
              "dp.DataNode", "SendMigrateData"),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.FInfo.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.SendAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<dp.Datanode.NodeMonitorReq,
      dp.Datanode.NodeMonitorAck> METHOD_NODE_MONITOR =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "dp.DataNode", "NodeMonitor"),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.NodeMonitorReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(dp.Datanode.NodeMonitorAck.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DataNodeStub newStub(io.grpc.Channel channel) {
    return new DataNodeStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DataNodeBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new DataNodeBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static DataNodeFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new DataNodeFutureStub(channel);
  }

  /**
   */
  public static abstract class DataNodeImplBase implements io.grpc.BindableService {

    /**
     */
    public void writeChunk(dp.Datanode.WriteChunkReq request,
        io.grpc.stub.StreamObserver<dp.Datanode.WriteChunkAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_WRITE_CHUNK, responseObserver);
    }

    /**
     */
    public void streamReadChunk(dp.Datanode.StreamReadChunkReq request,
        io.grpc.stub.StreamObserver<dp.Datanode.StreamReadChunkAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_STREAM_READ_CHUNK, responseObserver);
    }

    /**
     */
    public void deleteChunk(dp.Datanode.DeleteChunkReq request,
        io.grpc.stub.StreamObserver<dp.Datanode.DeleteChunkAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_CHUNK, responseObserver);
    }

    /**
     */
    public void datanodeHealthCheck(dp.Datanode.DatanodeHealthCheckReq request,
        io.grpc.stub.StreamObserver<dp.Datanode.DatanodeHealthCheckAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DATANODE_HEALTH_CHECK, responseObserver);
    }

    /**
     */
    public void recvMigrateMsg(dp.Datanode.RecvMigrateReq request,
        io.grpc.stub.StreamObserver<dp.Datanode.RecvMigrateAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_RECV_MIGRATE_MSG, responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<dp.Datanode.FInfo> sendMigrateData(
        io.grpc.stub.StreamObserver<dp.Datanode.SendAck> responseObserver) {
      return asyncUnimplementedStreamingCall(METHOD_SEND_MIGRATE_DATA, responseObserver);
    }

    /**
     */
    public void nodeMonitor(dp.Datanode.NodeMonitorReq request,
        io.grpc.stub.StreamObserver<dp.Datanode.NodeMonitorAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_NODE_MONITOR, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_WRITE_CHUNK,
            asyncUnaryCall(
              new MethodHandlers<
                dp.Datanode.WriteChunkReq,
                dp.Datanode.WriteChunkAck>(
                  this, METHODID_WRITE_CHUNK)))
          .addMethod(
            METHOD_STREAM_READ_CHUNK,
            asyncServerStreamingCall(
              new MethodHandlers<
                dp.Datanode.StreamReadChunkReq,
                dp.Datanode.StreamReadChunkAck>(
                  this, METHODID_STREAM_READ_CHUNK)))
          .addMethod(
            METHOD_DELETE_CHUNK,
            asyncUnaryCall(
              new MethodHandlers<
                dp.Datanode.DeleteChunkReq,
                dp.Datanode.DeleteChunkAck>(
                  this, METHODID_DELETE_CHUNK)))
          .addMethod(
            METHOD_DATANODE_HEALTH_CHECK,
            asyncUnaryCall(
              new MethodHandlers<
                dp.Datanode.DatanodeHealthCheckReq,
                dp.Datanode.DatanodeHealthCheckAck>(
                  this, METHODID_DATANODE_HEALTH_CHECK)))
          .addMethod(
            METHOD_RECV_MIGRATE_MSG,
            asyncUnaryCall(
              new MethodHandlers<
                dp.Datanode.RecvMigrateReq,
                dp.Datanode.RecvMigrateAck>(
                  this, METHODID_RECV_MIGRATE_MSG)))
          .addMethod(
            METHOD_SEND_MIGRATE_DATA,
            asyncClientStreamingCall(
              new MethodHandlers<
                dp.Datanode.FInfo,
                dp.Datanode.SendAck>(
                  this, METHODID_SEND_MIGRATE_DATA)))
          .addMethod(
            METHOD_NODE_MONITOR,
            asyncUnaryCall(
              new MethodHandlers<
                dp.Datanode.NodeMonitorReq,
                dp.Datanode.NodeMonitorAck>(
                  this, METHODID_NODE_MONITOR)))
          .build();
    }
  }

  /**
   */
  public static final class DataNodeStub extends io.grpc.stub.AbstractStub<DataNodeStub> {
    private DataNodeStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DataNodeStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DataNodeStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DataNodeStub(channel, callOptions);
    }

    /**
     */
    public void writeChunk(dp.Datanode.WriteChunkReq request,
        io.grpc.stub.StreamObserver<dp.Datanode.WriteChunkAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_WRITE_CHUNK, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void streamReadChunk(dp.Datanode.StreamReadChunkReq request,
        io.grpc.stub.StreamObserver<dp.Datanode.StreamReadChunkAck> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_STREAM_READ_CHUNK, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteChunk(dp.Datanode.DeleteChunkReq request,
        io.grpc.stub.StreamObserver<dp.Datanode.DeleteChunkAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_CHUNK, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void datanodeHealthCheck(dp.Datanode.DatanodeHealthCheckReq request,
        io.grpc.stub.StreamObserver<dp.Datanode.DatanodeHealthCheckAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DATANODE_HEALTH_CHECK, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void recvMigrateMsg(dp.Datanode.RecvMigrateReq request,
        io.grpc.stub.StreamObserver<dp.Datanode.RecvMigrateAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_RECV_MIGRATE_MSG, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<dp.Datanode.FInfo> sendMigrateData(
        io.grpc.stub.StreamObserver<dp.Datanode.SendAck> responseObserver) {
      return asyncClientStreamingCall(
          getChannel().newCall(METHOD_SEND_MIGRATE_DATA, getCallOptions()), responseObserver);
    }

    /**
     */
    public void nodeMonitor(dp.Datanode.NodeMonitorReq request,
        io.grpc.stub.StreamObserver<dp.Datanode.NodeMonitorAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_NODE_MONITOR, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DataNodeBlockingStub extends io.grpc.stub.AbstractStub<DataNodeBlockingStub> {
    private DataNodeBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DataNodeBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DataNodeBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DataNodeBlockingStub(channel, callOptions);
    }

    /**
     */
    public dp.Datanode.WriteChunkAck writeChunk(dp.Datanode.WriteChunkReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_WRITE_CHUNK, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<dp.Datanode.StreamReadChunkAck> streamReadChunk(
        dp.Datanode.StreamReadChunkReq request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_STREAM_READ_CHUNK, getCallOptions(), request);
    }

    /**
     */
    public dp.Datanode.DeleteChunkAck deleteChunk(dp.Datanode.DeleteChunkReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_CHUNK, getCallOptions(), request);
    }

    /**
     */
    public dp.Datanode.DatanodeHealthCheckAck datanodeHealthCheck(dp.Datanode.DatanodeHealthCheckReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DATANODE_HEALTH_CHECK, getCallOptions(), request);
    }

    /**
     */
    public dp.Datanode.RecvMigrateAck recvMigrateMsg(dp.Datanode.RecvMigrateReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_RECV_MIGRATE_MSG, getCallOptions(), request);
    }

    /**
     */
    public dp.Datanode.NodeMonitorAck nodeMonitor(dp.Datanode.NodeMonitorReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_NODE_MONITOR, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DataNodeFutureStub extends io.grpc.stub.AbstractStub<DataNodeFutureStub> {
    private DataNodeFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DataNodeFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DataNodeFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DataNodeFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<dp.Datanode.WriteChunkAck> writeChunk(
        dp.Datanode.WriteChunkReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_WRITE_CHUNK, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<dp.Datanode.DeleteChunkAck> deleteChunk(
        dp.Datanode.DeleteChunkReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_CHUNK, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<dp.Datanode.DatanodeHealthCheckAck> datanodeHealthCheck(
        dp.Datanode.DatanodeHealthCheckReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DATANODE_HEALTH_CHECK, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<dp.Datanode.RecvMigrateAck> recvMigrateMsg(
        dp.Datanode.RecvMigrateReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_RECV_MIGRATE_MSG, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<dp.Datanode.NodeMonitorAck> nodeMonitor(
        dp.Datanode.NodeMonitorReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_NODE_MONITOR, getCallOptions()), request);
    }
  }

  private static final int METHODID_WRITE_CHUNK = 0;
  private static final int METHODID_STREAM_READ_CHUNK = 1;
  private static final int METHODID_DELETE_CHUNK = 2;
  private static final int METHODID_DATANODE_HEALTH_CHECK = 3;
  private static final int METHODID_RECV_MIGRATE_MSG = 4;
  private static final int METHODID_NODE_MONITOR = 5;
  private static final int METHODID_SEND_MIGRATE_DATA = 6;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DataNodeImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(DataNodeImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_WRITE_CHUNK:
          serviceImpl.writeChunk((dp.Datanode.WriteChunkReq) request,
              (io.grpc.stub.StreamObserver<dp.Datanode.WriteChunkAck>) responseObserver);
          break;
        case METHODID_STREAM_READ_CHUNK:
          serviceImpl.streamReadChunk((dp.Datanode.StreamReadChunkReq) request,
              (io.grpc.stub.StreamObserver<dp.Datanode.StreamReadChunkAck>) responseObserver);
          break;
        case METHODID_DELETE_CHUNK:
          serviceImpl.deleteChunk((dp.Datanode.DeleteChunkReq) request,
              (io.grpc.stub.StreamObserver<dp.Datanode.DeleteChunkAck>) responseObserver);
          break;
        case METHODID_DATANODE_HEALTH_CHECK:
          serviceImpl.datanodeHealthCheck((dp.Datanode.DatanodeHealthCheckReq) request,
              (io.grpc.stub.StreamObserver<dp.Datanode.DatanodeHealthCheckAck>) responseObserver);
          break;
        case METHODID_RECV_MIGRATE_MSG:
          serviceImpl.recvMigrateMsg((dp.Datanode.RecvMigrateReq) request,
              (io.grpc.stub.StreamObserver<dp.Datanode.RecvMigrateAck>) responseObserver);
          break;
        case METHODID_NODE_MONITOR:
          serviceImpl.nodeMonitor((dp.Datanode.NodeMonitorReq) request,
              (io.grpc.stub.StreamObserver<dp.Datanode.NodeMonitorAck>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SEND_MIGRATE_DATA:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.sendMigrateData(
              (io.grpc.stub.StreamObserver<dp.Datanode.SendAck>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    return new io.grpc.ServiceDescriptor(SERVICE_NAME,
        METHOD_WRITE_CHUNK,
        METHOD_STREAM_READ_CHUNK,
        METHOD_DELETE_CHUNK,
        METHOD_DATANODE_HEALTH_CHECK,
        METHOD_RECV_MIGRATE_MSG,
        METHOD_SEND_MIGRATE_DATA,
        METHOD_NODE_MONITOR);
  }

}
