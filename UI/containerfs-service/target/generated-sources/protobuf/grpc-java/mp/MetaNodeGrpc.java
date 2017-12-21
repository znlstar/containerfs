package mp;

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
    comments = "Source: metanode.proto")
public class MetaNodeGrpc {

  private MetaNodeGrpc() {}

  public static final String SERVICE_NAME = "mp.MetaNode";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.Datanode,
      mp.Metanode.DatanodeRegistryAck> METHOD_DATANODE_REGISTRY =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "DatanodeRegistry"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.Datanode.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.DatanodeRegistryAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.DelDatanodeReq,
      mp.Metanode.DelDatanodeAck> METHOD_DEL_DATANODE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "DelDatanode"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.DelDatanodeReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.DelDatanodeAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.CreateVolReq,
      mp.Metanode.CreateVolAck> METHOD_CREATE_VOL =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "CreateVol"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.CreateVolReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.CreateVolAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.ExpandVolTSReq,
      mp.Metanode.ExpandVolTSAck> METHOD_EXPAND_VOL_TS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "ExpandVolTS"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.ExpandVolTSReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.ExpandVolTSAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.ExpandVolRSReq,
      mp.Metanode.ExpandVolRSAck> METHOD_EXPAND_VOL_RS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "ExpandVolRS"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.ExpandVolRSReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.ExpandVolRSAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.DelVolRSForExpandReq,
      mp.Metanode.DelVolRSForExpandAck> METHOD_DEL_VOL_RSFOR_EXPAND =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "DelVolRSForExpand"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.DelVolRSForExpandReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.DelVolRSForExpandAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.DeleteVolReq,
      mp.Metanode.DeleteVolAck> METHOD_DELETE_VOL =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "DeleteVol"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.DeleteVolReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.DeleteVolAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.MigrateReq,
      mp.Metanode.MigrateAck> METHOD_MIGRATE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "Migrate"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.MigrateReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.MigrateAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.GetMetaLeaderReq,
      mp.Metanode.GetMetaLeaderAck> METHOD_GET_META_LEADER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "GetMetaLeader"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.GetMetaLeaderReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.GetMetaLeaderAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.CreateNameSpaceReq,
      mp.Metanode.CreateNameSpaceAck> METHOD_CREATE_NAME_SPACE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "CreateNameSpace"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.CreateNameSpaceReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.CreateNameSpaceAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.ExpandNameSpaceReq,
      mp.Metanode.ExpandNameSpaceAck> METHOD_EXPAND_NAME_SPACE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "ExpandNameSpace"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.ExpandNameSpaceReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.ExpandNameSpaceAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.SnapShootNameSpaceReq,
      mp.Metanode.SnapShootNameSpaceAck> METHOD_SNAP_SHOOT_NAME_SPACE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "SnapShootNameSpace"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.SnapShootNameSpaceReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.SnapShootNameSpaceAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.DeleteNameSpaceReq,
      mp.Metanode.DeleteNameSpaceAck> METHOD_DELETE_NAME_SPACE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "DeleteNameSpace"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.DeleteNameSpaceReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.DeleteNameSpaceAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.GetFSInfoReq,
      mp.Metanode.GetFSInfoAck> METHOD_GET_FSINFO =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "GetFSInfo"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.GetFSInfoReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.GetFSInfoAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.CreateDirDirectReq,
      mp.Metanode.CreateDirDirectAck> METHOD_CREATE_DIR_DIRECT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "CreateDirDirect"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.CreateDirDirectReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.CreateDirDirectAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.StatDirectReq,
      mp.Metanode.StatDirectAck> METHOD_STAT_DIRECT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "StatDirect"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.StatDirectReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.StatDirectAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.GetInodeInfoDirectReq,
      mp.Metanode.GetInodeInfoDirectAck> METHOD_GET_INODE_INFO_DIRECT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "GetInodeInfoDirect"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.GetInodeInfoDirectReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.GetInodeInfoDirectAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.ListDirectReq,
      mp.Metanode.ListDirectAck> METHOD_LIST_DIRECT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "ListDirect"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.ListDirectReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.ListDirectAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.DeleteDirDirectReq,
      mp.Metanode.DeleteDirDirectAck> METHOD_DELETE_DIR_DIRECT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "DeleteDirDirect"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.DeleteDirDirectReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.DeleteDirDirectAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.RenameDirectReq,
      mp.Metanode.RenameDirectAck> METHOD_RENAME_DIRECT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "RenameDirect"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.RenameDirectReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.RenameDirectAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.CreateFileDirectReq,
      mp.Metanode.CreateFileDirectAck> METHOD_CREATE_FILE_DIRECT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "CreateFileDirect"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.CreateFileDirectReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.CreateFileDirectAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.DeleteFileDirectReq,
      mp.Metanode.DeleteFileDirectAck> METHOD_DELETE_FILE_DIRECT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "DeleteFileDirect"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.DeleteFileDirectReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.DeleteFileDirectAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.GetFileChunksDirectReq,
      mp.Metanode.GetFileChunksDirectAck> METHOD_GET_FILE_CHUNKS_DIRECT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "GetFileChunksDirect"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.GetFileChunksDirectReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.GetFileChunksDirectAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.AllocateChunkReq,
      mp.Metanode.AllocateChunkAck> METHOD_ALLOCATE_CHUNK =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "AllocateChunk"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.AllocateChunkReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.AllocateChunkAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.SyncChunkReq,
      mp.Metanode.SyncChunkAck> METHOD_SYNC_CHUNK =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "SyncChunk"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.SyncChunkReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.SyncChunkAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.ClusterInfoReq,
      mp.Metanode.ClusterInfoAck> METHOD_CLUSTER_INFO =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "ClusterInfo"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.ClusterInfoReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.ClusterInfoAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.GetAllDatanodeReq,
      mp.Metanode.GetAllDatanodeAck> METHOD_GET_ALL_DATANODE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "GetAllDatanode"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.GetAllDatanodeReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.GetAllDatanodeAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.MetaNodeInfoReq,
      mp.Metanode.MetaNodeInfoAck> METHOD_META_NODE_INFO =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "MetaNodeInfo"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.MetaNodeInfoReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.MetaNodeInfoAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.VolumeInfosReq,
      mp.Metanode.VolumeInfosAck> METHOD_VOLUME_INFOS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "VolumeInfos"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.VolumeInfosReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.VolumeInfosAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.GetVolInfoReq,
      mp.Metanode.GetVolInfoAck> METHOD_GET_VOL_INFO =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "GetVolInfo"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.GetVolInfoReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.GetVolInfoAck.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<mp.Metanode.NodeMonitorReq,
      mp.Metanode.NodeMonitorAck> METHOD_NODE_MONITOR =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "mp.MetaNode", "NodeMonitor"),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.NodeMonitorReq.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(mp.Metanode.NodeMonitorAck.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MetaNodeStub newStub(io.grpc.Channel channel) {
    return new MetaNodeStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MetaNodeBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new MetaNodeBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static MetaNodeFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new MetaNodeFutureStub(channel);
  }

  /**
   */
  public static abstract class MetaNodeImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * cluster opt
     * </pre>
     */
    public void datanodeRegistry(mp.Metanode.Datanode request,
        io.grpc.stub.StreamObserver<mp.Metanode.DatanodeRegistryAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DATANODE_REGISTRY, responseObserver);
    }

    /**
     */
    public void delDatanode(mp.Metanode.DelDatanodeReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.DelDatanodeAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DEL_DATANODE, responseObserver);
    }

    /**
     */
    public void createVol(mp.Metanode.CreateVolReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.CreateVolAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CREATE_VOL, responseObserver);
    }

    /**
     */
    public void expandVolTS(mp.Metanode.ExpandVolTSReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.ExpandVolTSAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_EXPAND_VOL_TS, responseObserver);
    }

    /**
     */
    public void expandVolRS(mp.Metanode.ExpandVolRSReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.ExpandVolRSAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_EXPAND_VOL_RS, responseObserver);
    }

    /**
     */
    public void delVolRSForExpand(mp.Metanode.DelVolRSForExpandReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.DelVolRSForExpandAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DEL_VOL_RSFOR_EXPAND, responseObserver);
    }

    /**
     */
    public void deleteVol(mp.Metanode.DeleteVolReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.DeleteVolAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_VOL, responseObserver);
    }

    /**
     */
    public void migrate(mp.Metanode.MigrateReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.MigrateAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_MIGRATE, responseObserver);
    }

    /**
     * <pre>
     * namespace opt
     * </pre>
     */
    public void getMetaLeader(mp.Metanode.GetMetaLeaderReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.GetMetaLeaderAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_META_LEADER, responseObserver);
    }

    /**
     */
    public void createNameSpace(mp.Metanode.CreateNameSpaceReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.CreateNameSpaceAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CREATE_NAME_SPACE, responseObserver);
    }

    /**
     */
    public void expandNameSpace(mp.Metanode.ExpandNameSpaceReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.ExpandNameSpaceAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_EXPAND_NAME_SPACE, responseObserver);
    }

    /**
     */
    public void snapShootNameSpace(mp.Metanode.SnapShootNameSpaceReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.SnapShootNameSpaceAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SNAP_SHOOT_NAME_SPACE, responseObserver);
    }

    /**
     */
    public void deleteNameSpace(mp.Metanode.DeleteNameSpaceReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.DeleteNameSpaceAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_NAME_SPACE, responseObserver);
    }

    /**
     */
    public void getFSInfo(mp.Metanode.GetFSInfoReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.GetFSInfoAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_FSINFO, responseObserver);
    }

    /**
     * <pre>
     * fs meta opt
     * </pre>
     */
    public void createDirDirect(mp.Metanode.CreateDirDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.CreateDirDirectAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CREATE_DIR_DIRECT, responseObserver);
    }

    /**
     */
    public void statDirect(mp.Metanode.StatDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.StatDirectAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_STAT_DIRECT, responseObserver);
    }

    /**
     */
    public void getInodeInfoDirect(mp.Metanode.GetInodeInfoDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.GetInodeInfoDirectAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_INODE_INFO_DIRECT, responseObserver);
    }

    /**
     */
    public void listDirect(mp.Metanode.ListDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.ListDirectAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_LIST_DIRECT, responseObserver);
    }

    /**
     */
    public void deleteDirDirect(mp.Metanode.DeleteDirDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.DeleteDirDirectAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_DIR_DIRECT, responseObserver);
    }

    /**
     */
    public void renameDirect(mp.Metanode.RenameDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.RenameDirectAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_RENAME_DIRECT, responseObserver);
    }

    /**
     */
    public void createFileDirect(mp.Metanode.CreateFileDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.CreateFileDirectAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CREATE_FILE_DIRECT, responseObserver);
    }

    /**
     */
    public void deleteFileDirect(mp.Metanode.DeleteFileDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.DeleteFileDirectAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_FILE_DIRECT, responseObserver);
    }

    /**
     */
    public void getFileChunksDirect(mp.Metanode.GetFileChunksDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.GetFileChunksDirectAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_FILE_CHUNKS_DIRECT, responseObserver);
    }

    /**
     */
    public void allocateChunk(mp.Metanode.AllocateChunkReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.AllocateChunkAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_ALLOCATE_CHUNK, responseObserver);
    }

    /**
     */
    public void syncChunk(mp.Metanode.SyncChunkReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.SyncChunkAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SYNC_CHUNK, responseObserver);
    }

    /**
     * <pre>
     * Web Info
     * </pre>
     */
    public void clusterInfo(mp.Metanode.ClusterInfoReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.ClusterInfoAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CLUSTER_INFO, responseObserver);
    }

    /**
     */
    public void getAllDatanode(mp.Metanode.GetAllDatanodeReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.GetAllDatanodeAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_ALL_DATANODE, responseObserver);
    }

    /**
     */
    public void metaNodeInfo(mp.Metanode.MetaNodeInfoReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.MetaNodeInfoAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_META_NODE_INFO, responseObserver);
    }

    /**
     */
    public void volumeInfos(mp.Metanode.VolumeInfosReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.VolumeInfosAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_VOLUME_INFOS, responseObserver);
    }

    /**
     */
    public void getVolInfo(mp.Metanode.GetVolInfoReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.GetVolInfoAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_VOL_INFO, responseObserver);
    }

    /**
     */
    public void nodeMonitor(mp.Metanode.NodeMonitorReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.NodeMonitorAck> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_NODE_MONITOR, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_DATANODE_REGISTRY,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.Datanode,
                mp.Metanode.DatanodeRegistryAck>(
                  this, METHODID_DATANODE_REGISTRY)))
          .addMethod(
            METHOD_DEL_DATANODE,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.DelDatanodeReq,
                mp.Metanode.DelDatanodeAck>(
                  this, METHODID_DEL_DATANODE)))
          .addMethod(
            METHOD_CREATE_VOL,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.CreateVolReq,
                mp.Metanode.CreateVolAck>(
                  this, METHODID_CREATE_VOL)))
          .addMethod(
            METHOD_EXPAND_VOL_TS,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.ExpandVolTSReq,
                mp.Metanode.ExpandVolTSAck>(
                  this, METHODID_EXPAND_VOL_TS)))
          .addMethod(
            METHOD_EXPAND_VOL_RS,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.ExpandVolRSReq,
                mp.Metanode.ExpandVolRSAck>(
                  this, METHODID_EXPAND_VOL_RS)))
          .addMethod(
            METHOD_DEL_VOL_RSFOR_EXPAND,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.DelVolRSForExpandReq,
                mp.Metanode.DelVolRSForExpandAck>(
                  this, METHODID_DEL_VOL_RSFOR_EXPAND)))
          .addMethod(
            METHOD_DELETE_VOL,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.DeleteVolReq,
                mp.Metanode.DeleteVolAck>(
                  this, METHODID_DELETE_VOL)))
          .addMethod(
            METHOD_MIGRATE,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.MigrateReq,
                mp.Metanode.MigrateAck>(
                  this, METHODID_MIGRATE)))
          .addMethod(
            METHOD_GET_META_LEADER,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.GetMetaLeaderReq,
                mp.Metanode.GetMetaLeaderAck>(
                  this, METHODID_GET_META_LEADER)))
          .addMethod(
            METHOD_CREATE_NAME_SPACE,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.CreateNameSpaceReq,
                mp.Metanode.CreateNameSpaceAck>(
                  this, METHODID_CREATE_NAME_SPACE)))
          .addMethod(
            METHOD_EXPAND_NAME_SPACE,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.ExpandNameSpaceReq,
                mp.Metanode.ExpandNameSpaceAck>(
                  this, METHODID_EXPAND_NAME_SPACE)))
          .addMethod(
            METHOD_SNAP_SHOOT_NAME_SPACE,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.SnapShootNameSpaceReq,
                mp.Metanode.SnapShootNameSpaceAck>(
                  this, METHODID_SNAP_SHOOT_NAME_SPACE)))
          .addMethod(
            METHOD_DELETE_NAME_SPACE,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.DeleteNameSpaceReq,
                mp.Metanode.DeleteNameSpaceAck>(
                  this, METHODID_DELETE_NAME_SPACE)))
          .addMethod(
            METHOD_GET_FSINFO,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.GetFSInfoReq,
                mp.Metanode.GetFSInfoAck>(
                  this, METHODID_GET_FSINFO)))
          .addMethod(
            METHOD_CREATE_DIR_DIRECT,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.CreateDirDirectReq,
                mp.Metanode.CreateDirDirectAck>(
                  this, METHODID_CREATE_DIR_DIRECT)))
          .addMethod(
            METHOD_STAT_DIRECT,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.StatDirectReq,
                mp.Metanode.StatDirectAck>(
                  this, METHODID_STAT_DIRECT)))
          .addMethod(
            METHOD_GET_INODE_INFO_DIRECT,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.GetInodeInfoDirectReq,
                mp.Metanode.GetInodeInfoDirectAck>(
                  this, METHODID_GET_INODE_INFO_DIRECT)))
          .addMethod(
            METHOD_LIST_DIRECT,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.ListDirectReq,
                mp.Metanode.ListDirectAck>(
                  this, METHODID_LIST_DIRECT)))
          .addMethod(
            METHOD_DELETE_DIR_DIRECT,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.DeleteDirDirectReq,
                mp.Metanode.DeleteDirDirectAck>(
                  this, METHODID_DELETE_DIR_DIRECT)))
          .addMethod(
            METHOD_RENAME_DIRECT,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.RenameDirectReq,
                mp.Metanode.RenameDirectAck>(
                  this, METHODID_RENAME_DIRECT)))
          .addMethod(
            METHOD_CREATE_FILE_DIRECT,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.CreateFileDirectReq,
                mp.Metanode.CreateFileDirectAck>(
                  this, METHODID_CREATE_FILE_DIRECT)))
          .addMethod(
            METHOD_DELETE_FILE_DIRECT,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.DeleteFileDirectReq,
                mp.Metanode.DeleteFileDirectAck>(
                  this, METHODID_DELETE_FILE_DIRECT)))
          .addMethod(
            METHOD_GET_FILE_CHUNKS_DIRECT,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.GetFileChunksDirectReq,
                mp.Metanode.GetFileChunksDirectAck>(
                  this, METHODID_GET_FILE_CHUNKS_DIRECT)))
          .addMethod(
            METHOD_ALLOCATE_CHUNK,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.AllocateChunkReq,
                mp.Metanode.AllocateChunkAck>(
                  this, METHODID_ALLOCATE_CHUNK)))
          .addMethod(
            METHOD_SYNC_CHUNK,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.SyncChunkReq,
                mp.Metanode.SyncChunkAck>(
                  this, METHODID_SYNC_CHUNK)))
          .addMethod(
            METHOD_CLUSTER_INFO,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.ClusterInfoReq,
                mp.Metanode.ClusterInfoAck>(
                  this, METHODID_CLUSTER_INFO)))
          .addMethod(
            METHOD_GET_ALL_DATANODE,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.GetAllDatanodeReq,
                mp.Metanode.GetAllDatanodeAck>(
                  this, METHODID_GET_ALL_DATANODE)))
          .addMethod(
            METHOD_META_NODE_INFO,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.MetaNodeInfoReq,
                mp.Metanode.MetaNodeInfoAck>(
                  this, METHODID_META_NODE_INFO)))
          .addMethod(
            METHOD_VOLUME_INFOS,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.VolumeInfosReq,
                mp.Metanode.VolumeInfosAck>(
                  this, METHODID_VOLUME_INFOS)))
          .addMethod(
            METHOD_GET_VOL_INFO,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.GetVolInfoReq,
                mp.Metanode.GetVolInfoAck>(
                  this, METHODID_GET_VOL_INFO)))
          .addMethod(
            METHOD_NODE_MONITOR,
            asyncUnaryCall(
              new MethodHandlers<
                mp.Metanode.NodeMonitorReq,
                mp.Metanode.NodeMonitorAck>(
                  this, METHODID_NODE_MONITOR)))
          .build();
    }
  }

  /**
   */
  public static final class MetaNodeStub extends io.grpc.stub.AbstractStub<MetaNodeStub> {
    private MetaNodeStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaNodeStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaNodeStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetaNodeStub(channel, callOptions);
    }

    /**
     * <pre>
     * cluster opt
     * </pre>
     */
    public void datanodeRegistry(mp.Metanode.Datanode request,
        io.grpc.stub.StreamObserver<mp.Metanode.DatanodeRegistryAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DATANODE_REGISTRY, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void delDatanode(mp.Metanode.DelDatanodeReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.DelDatanodeAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DEL_DATANODE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createVol(mp.Metanode.CreateVolReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.CreateVolAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_VOL, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void expandVolTS(mp.Metanode.ExpandVolTSReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.ExpandVolTSAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_EXPAND_VOL_TS, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void expandVolRS(mp.Metanode.ExpandVolRSReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.ExpandVolRSAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_EXPAND_VOL_RS, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void delVolRSForExpand(mp.Metanode.DelVolRSForExpandReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.DelVolRSForExpandAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DEL_VOL_RSFOR_EXPAND, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteVol(mp.Metanode.DeleteVolReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.DeleteVolAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_VOL, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void migrate(mp.Metanode.MigrateReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.MigrateAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_MIGRATE, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * namespace opt
     * </pre>
     */
    public void getMetaLeader(mp.Metanode.GetMetaLeaderReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.GetMetaLeaderAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_META_LEADER, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createNameSpace(mp.Metanode.CreateNameSpaceReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.CreateNameSpaceAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_NAME_SPACE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void expandNameSpace(mp.Metanode.ExpandNameSpaceReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.ExpandNameSpaceAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_EXPAND_NAME_SPACE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void snapShootNameSpace(mp.Metanode.SnapShootNameSpaceReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.SnapShootNameSpaceAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SNAP_SHOOT_NAME_SPACE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteNameSpace(mp.Metanode.DeleteNameSpaceReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.DeleteNameSpaceAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_NAME_SPACE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getFSInfo(mp.Metanode.GetFSInfoReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.GetFSInfoAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_FSINFO, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * fs meta opt
     * </pre>
     */
    public void createDirDirect(mp.Metanode.CreateDirDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.CreateDirDirectAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_DIR_DIRECT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void statDirect(mp.Metanode.StatDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.StatDirectAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_STAT_DIRECT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getInodeInfoDirect(mp.Metanode.GetInodeInfoDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.GetInodeInfoDirectAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_INODE_INFO_DIRECT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listDirect(mp.Metanode.ListDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.ListDirectAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_LIST_DIRECT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteDirDirect(mp.Metanode.DeleteDirDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.DeleteDirDirectAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_DIR_DIRECT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void renameDirect(mp.Metanode.RenameDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.RenameDirectAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_RENAME_DIRECT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createFileDirect(mp.Metanode.CreateFileDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.CreateFileDirectAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_FILE_DIRECT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteFileDirect(mp.Metanode.DeleteFileDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.DeleteFileDirectAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_FILE_DIRECT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getFileChunksDirect(mp.Metanode.GetFileChunksDirectReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.GetFileChunksDirectAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_FILE_CHUNKS_DIRECT, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void allocateChunk(mp.Metanode.AllocateChunkReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.AllocateChunkAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_ALLOCATE_CHUNK, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void syncChunk(mp.Metanode.SyncChunkReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.SyncChunkAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SYNC_CHUNK, getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Web Info
     * </pre>
     */
    public void clusterInfo(mp.Metanode.ClusterInfoReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.ClusterInfoAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CLUSTER_INFO, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getAllDatanode(mp.Metanode.GetAllDatanodeReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.GetAllDatanodeAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_ALL_DATANODE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void metaNodeInfo(mp.Metanode.MetaNodeInfoReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.MetaNodeInfoAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_META_NODE_INFO, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void volumeInfos(mp.Metanode.VolumeInfosReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.VolumeInfosAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_VOLUME_INFOS, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getVolInfo(mp.Metanode.GetVolInfoReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.GetVolInfoAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_VOL_INFO, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void nodeMonitor(mp.Metanode.NodeMonitorReq request,
        io.grpc.stub.StreamObserver<mp.Metanode.NodeMonitorAck> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_NODE_MONITOR, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class MetaNodeBlockingStub extends io.grpc.stub.AbstractStub<MetaNodeBlockingStub> {
    private MetaNodeBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaNodeBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaNodeBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetaNodeBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * cluster opt
     * </pre>
     */
    public mp.Metanode.DatanodeRegistryAck datanodeRegistry(mp.Metanode.Datanode request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DATANODE_REGISTRY, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.DelDatanodeAck delDatanode(mp.Metanode.DelDatanodeReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DEL_DATANODE, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.CreateVolAck createVol(mp.Metanode.CreateVolReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_VOL, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.ExpandVolTSAck expandVolTS(mp.Metanode.ExpandVolTSReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_EXPAND_VOL_TS, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.ExpandVolRSAck expandVolRS(mp.Metanode.ExpandVolRSReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_EXPAND_VOL_RS, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.DelVolRSForExpandAck delVolRSForExpand(mp.Metanode.DelVolRSForExpandReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DEL_VOL_RSFOR_EXPAND, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.DeleteVolAck deleteVol(mp.Metanode.DeleteVolReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_VOL, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.MigrateAck migrate(mp.Metanode.MigrateReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_MIGRATE, getCallOptions(), request);
    }

    /**
     * <pre>
     * namespace opt
     * </pre>
     */
    public mp.Metanode.GetMetaLeaderAck getMetaLeader(mp.Metanode.GetMetaLeaderReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_META_LEADER, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.CreateNameSpaceAck createNameSpace(mp.Metanode.CreateNameSpaceReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_NAME_SPACE, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.ExpandNameSpaceAck expandNameSpace(mp.Metanode.ExpandNameSpaceReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_EXPAND_NAME_SPACE, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.SnapShootNameSpaceAck snapShootNameSpace(mp.Metanode.SnapShootNameSpaceReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SNAP_SHOOT_NAME_SPACE, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.DeleteNameSpaceAck deleteNameSpace(mp.Metanode.DeleteNameSpaceReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_NAME_SPACE, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.GetFSInfoAck getFSInfo(mp.Metanode.GetFSInfoReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_FSINFO, getCallOptions(), request);
    }

    /**
     * <pre>
     * fs meta opt
     * </pre>
     */
    public mp.Metanode.CreateDirDirectAck createDirDirect(mp.Metanode.CreateDirDirectReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_DIR_DIRECT, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.StatDirectAck statDirect(mp.Metanode.StatDirectReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_STAT_DIRECT, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.GetInodeInfoDirectAck getInodeInfoDirect(mp.Metanode.GetInodeInfoDirectReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_INODE_INFO_DIRECT, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.ListDirectAck listDirect(mp.Metanode.ListDirectReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_LIST_DIRECT, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.DeleteDirDirectAck deleteDirDirect(mp.Metanode.DeleteDirDirectReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_DIR_DIRECT, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.RenameDirectAck renameDirect(mp.Metanode.RenameDirectReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_RENAME_DIRECT, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.CreateFileDirectAck createFileDirect(mp.Metanode.CreateFileDirectReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_FILE_DIRECT, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.DeleteFileDirectAck deleteFileDirect(mp.Metanode.DeleteFileDirectReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_FILE_DIRECT, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.GetFileChunksDirectAck getFileChunksDirect(mp.Metanode.GetFileChunksDirectReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_FILE_CHUNKS_DIRECT, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.AllocateChunkAck allocateChunk(mp.Metanode.AllocateChunkReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_ALLOCATE_CHUNK, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.SyncChunkAck syncChunk(mp.Metanode.SyncChunkReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SYNC_CHUNK, getCallOptions(), request);
    }

    /**
     * <pre>
     * Web Info
     * </pre>
     */
    public mp.Metanode.ClusterInfoAck clusterInfo(mp.Metanode.ClusterInfoReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CLUSTER_INFO, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.GetAllDatanodeAck getAllDatanode(mp.Metanode.GetAllDatanodeReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_ALL_DATANODE, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.MetaNodeInfoAck metaNodeInfo(mp.Metanode.MetaNodeInfoReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_META_NODE_INFO, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.VolumeInfosAck volumeInfos(mp.Metanode.VolumeInfosReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_VOLUME_INFOS, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.GetVolInfoAck getVolInfo(mp.Metanode.GetVolInfoReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_VOL_INFO, getCallOptions(), request);
    }

    /**
     */
    public mp.Metanode.NodeMonitorAck nodeMonitor(mp.Metanode.NodeMonitorReq request) {
      return blockingUnaryCall(
          getChannel(), METHOD_NODE_MONITOR, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class MetaNodeFutureStub extends io.grpc.stub.AbstractStub<MetaNodeFutureStub> {
    private MetaNodeFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MetaNodeFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetaNodeFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MetaNodeFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * cluster opt
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.DatanodeRegistryAck> datanodeRegistry(
        mp.Metanode.Datanode request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DATANODE_REGISTRY, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.DelDatanodeAck> delDatanode(
        mp.Metanode.DelDatanodeReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DEL_DATANODE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.CreateVolAck> createVol(
        mp.Metanode.CreateVolReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_VOL, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.ExpandVolTSAck> expandVolTS(
        mp.Metanode.ExpandVolTSReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_EXPAND_VOL_TS, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.ExpandVolRSAck> expandVolRS(
        mp.Metanode.ExpandVolRSReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_EXPAND_VOL_RS, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.DelVolRSForExpandAck> delVolRSForExpand(
        mp.Metanode.DelVolRSForExpandReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DEL_VOL_RSFOR_EXPAND, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.DeleteVolAck> deleteVol(
        mp.Metanode.DeleteVolReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_VOL, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.MigrateAck> migrate(
        mp.Metanode.MigrateReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_MIGRATE, getCallOptions()), request);
    }

    /**
     * <pre>
     * namespace opt
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.GetMetaLeaderAck> getMetaLeader(
        mp.Metanode.GetMetaLeaderReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_META_LEADER, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.CreateNameSpaceAck> createNameSpace(
        mp.Metanode.CreateNameSpaceReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_NAME_SPACE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.ExpandNameSpaceAck> expandNameSpace(
        mp.Metanode.ExpandNameSpaceReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_EXPAND_NAME_SPACE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.SnapShootNameSpaceAck> snapShootNameSpace(
        mp.Metanode.SnapShootNameSpaceReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SNAP_SHOOT_NAME_SPACE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.DeleteNameSpaceAck> deleteNameSpace(
        mp.Metanode.DeleteNameSpaceReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_NAME_SPACE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.GetFSInfoAck> getFSInfo(
        mp.Metanode.GetFSInfoReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_FSINFO, getCallOptions()), request);
    }

    /**
     * <pre>
     * fs meta opt
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.CreateDirDirectAck> createDirDirect(
        mp.Metanode.CreateDirDirectReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_DIR_DIRECT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.StatDirectAck> statDirect(
        mp.Metanode.StatDirectReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_STAT_DIRECT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.GetInodeInfoDirectAck> getInodeInfoDirect(
        mp.Metanode.GetInodeInfoDirectReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_INODE_INFO_DIRECT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.ListDirectAck> listDirect(
        mp.Metanode.ListDirectReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_LIST_DIRECT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.DeleteDirDirectAck> deleteDirDirect(
        mp.Metanode.DeleteDirDirectReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_DIR_DIRECT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.RenameDirectAck> renameDirect(
        mp.Metanode.RenameDirectReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_RENAME_DIRECT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.CreateFileDirectAck> createFileDirect(
        mp.Metanode.CreateFileDirectReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_FILE_DIRECT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.DeleteFileDirectAck> deleteFileDirect(
        mp.Metanode.DeleteFileDirectReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_FILE_DIRECT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.GetFileChunksDirectAck> getFileChunksDirect(
        mp.Metanode.GetFileChunksDirectReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_FILE_CHUNKS_DIRECT, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.AllocateChunkAck> allocateChunk(
        mp.Metanode.AllocateChunkReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_ALLOCATE_CHUNK, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.SyncChunkAck> syncChunk(
        mp.Metanode.SyncChunkReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SYNC_CHUNK, getCallOptions()), request);
    }

    /**
     * <pre>
     * Web Info
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.ClusterInfoAck> clusterInfo(
        mp.Metanode.ClusterInfoReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CLUSTER_INFO, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.GetAllDatanodeAck> getAllDatanode(
        mp.Metanode.GetAllDatanodeReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_ALL_DATANODE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.MetaNodeInfoAck> metaNodeInfo(
        mp.Metanode.MetaNodeInfoReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_META_NODE_INFO, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.VolumeInfosAck> volumeInfos(
        mp.Metanode.VolumeInfosReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_VOLUME_INFOS, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.GetVolInfoAck> getVolInfo(
        mp.Metanode.GetVolInfoReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_VOL_INFO, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<mp.Metanode.NodeMonitorAck> nodeMonitor(
        mp.Metanode.NodeMonitorReq request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_NODE_MONITOR, getCallOptions()), request);
    }
  }

  private static final int METHODID_DATANODE_REGISTRY = 0;
  private static final int METHODID_DEL_DATANODE = 1;
  private static final int METHODID_CREATE_VOL = 2;
  private static final int METHODID_EXPAND_VOL_TS = 3;
  private static final int METHODID_EXPAND_VOL_RS = 4;
  private static final int METHODID_DEL_VOL_RSFOR_EXPAND = 5;
  private static final int METHODID_DELETE_VOL = 6;
  private static final int METHODID_MIGRATE = 7;
  private static final int METHODID_GET_META_LEADER = 8;
  private static final int METHODID_CREATE_NAME_SPACE = 9;
  private static final int METHODID_EXPAND_NAME_SPACE = 10;
  private static final int METHODID_SNAP_SHOOT_NAME_SPACE = 11;
  private static final int METHODID_DELETE_NAME_SPACE = 12;
  private static final int METHODID_GET_FSINFO = 13;
  private static final int METHODID_CREATE_DIR_DIRECT = 14;
  private static final int METHODID_STAT_DIRECT = 15;
  private static final int METHODID_GET_INODE_INFO_DIRECT = 16;
  private static final int METHODID_LIST_DIRECT = 17;
  private static final int METHODID_DELETE_DIR_DIRECT = 18;
  private static final int METHODID_RENAME_DIRECT = 19;
  private static final int METHODID_CREATE_FILE_DIRECT = 20;
  private static final int METHODID_DELETE_FILE_DIRECT = 21;
  private static final int METHODID_GET_FILE_CHUNKS_DIRECT = 22;
  private static final int METHODID_ALLOCATE_CHUNK = 23;
  private static final int METHODID_SYNC_CHUNK = 24;
  private static final int METHODID_CLUSTER_INFO = 25;
  private static final int METHODID_GET_ALL_DATANODE = 26;
  private static final int METHODID_META_NODE_INFO = 27;
  private static final int METHODID_VOLUME_INFOS = 28;
  private static final int METHODID_GET_VOL_INFO = 29;
  private static final int METHODID_NODE_MONITOR = 30;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MetaNodeImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(MetaNodeImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_DATANODE_REGISTRY:
          serviceImpl.datanodeRegistry((mp.Metanode.Datanode) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.DatanodeRegistryAck>) responseObserver);
          break;
        case METHODID_DEL_DATANODE:
          serviceImpl.delDatanode((mp.Metanode.DelDatanodeReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.DelDatanodeAck>) responseObserver);
          break;
        case METHODID_CREATE_VOL:
          serviceImpl.createVol((mp.Metanode.CreateVolReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.CreateVolAck>) responseObserver);
          break;
        case METHODID_EXPAND_VOL_TS:
          serviceImpl.expandVolTS((mp.Metanode.ExpandVolTSReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.ExpandVolTSAck>) responseObserver);
          break;
        case METHODID_EXPAND_VOL_RS:
          serviceImpl.expandVolRS((mp.Metanode.ExpandVolRSReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.ExpandVolRSAck>) responseObserver);
          break;
        case METHODID_DEL_VOL_RSFOR_EXPAND:
          serviceImpl.delVolRSForExpand((mp.Metanode.DelVolRSForExpandReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.DelVolRSForExpandAck>) responseObserver);
          break;
        case METHODID_DELETE_VOL:
          serviceImpl.deleteVol((mp.Metanode.DeleteVolReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.DeleteVolAck>) responseObserver);
          break;
        case METHODID_MIGRATE:
          serviceImpl.migrate((mp.Metanode.MigrateReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.MigrateAck>) responseObserver);
          break;
        case METHODID_GET_META_LEADER:
          serviceImpl.getMetaLeader((mp.Metanode.GetMetaLeaderReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.GetMetaLeaderAck>) responseObserver);
          break;
        case METHODID_CREATE_NAME_SPACE:
          serviceImpl.createNameSpace((mp.Metanode.CreateNameSpaceReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.CreateNameSpaceAck>) responseObserver);
          break;
        case METHODID_EXPAND_NAME_SPACE:
          serviceImpl.expandNameSpace((mp.Metanode.ExpandNameSpaceReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.ExpandNameSpaceAck>) responseObserver);
          break;
        case METHODID_SNAP_SHOOT_NAME_SPACE:
          serviceImpl.snapShootNameSpace((mp.Metanode.SnapShootNameSpaceReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.SnapShootNameSpaceAck>) responseObserver);
          break;
        case METHODID_DELETE_NAME_SPACE:
          serviceImpl.deleteNameSpace((mp.Metanode.DeleteNameSpaceReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.DeleteNameSpaceAck>) responseObserver);
          break;
        case METHODID_GET_FSINFO:
          serviceImpl.getFSInfo((mp.Metanode.GetFSInfoReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.GetFSInfoAck>) responseObserver);
          break;
        case METHODID_CREATE_DIR_DIRECT:
          serviceImpl.createDirDirect((mp.Metanode.CreateDirDirectReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.CreateDirDirectAck>) responseObserver);
          break;
        case METHODID_STAT_DIRECT:
          serviceImpl.statDirect((mp.Metanode.StatDirectReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.StatDirectAck>) responseObserver);
          break;
        case METHODID_GET_INODE_INFO_DIRECT:
          serviceImpl.getInodeInfoDirect((mp.Metanode.GetInodeInfoDirectReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.GetInodeInfoDirectAck>) responseObserver);
          break;
        case METHODID_LIST_DIRECT:
          serviceImpl.listDirect((mp.Metanode.ListDirectReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.ListDirectAck>) responseObserver);
          break;
        case METHODID_DELETE_DIR_DIRECT:
          serviceImpl.deleteDirDirect((mp.Metanode.DeleteDirDirectReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.DeleteDirDirectAck>) responseObserver);
          break;
        case METHODID_RENAME_DIRECT:
          serviceImpl.renameDirect((mp.Metanode.RenameDirectReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.RenameDirectAck>) responseObserver);
          break;
        case METHODID_CREATE_FILE_DIRECT:
          serviceImpl.createFileDirect((mp.Metanode.CreateFileDirectReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.CreateFileDirectAck>) responseObserver);
          break;
        case METHODID_DELETE_FILE_DIRECT:
          serviceImpl.deleteFileDirect((mp.Metanode.DeleteFileDirectReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.DeleteFileDirectAck>) responseObserver);
          break;
        case METHODID_GET_FILE_CHUNKS_DIRECT:
          serviceImpl.getFileChunksDirect((mp.Metanode.GetFileChunksDirectReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.GetFileChunksDirectAck>) responseObserver);
          break;
        case METHODID_ALLOCATE_CHUNK:
          serviceImpl.allocateChunk((mp.Metanode.AllocateChunkReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.AllocateChunkAck>) responseObserver);
          break;
        case METHODID_SYNC_CHUNK:
          serviceImpl.syncChunk((mp.Metanode.SyncChunkReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.SyncChunkAck>) responseObserver);
          break;
        case METHODID_CLUSTER_INFO:
          serviceImpl.clusterInfo((mp.Metanode.ClusterInfoReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.ClusterInfoAck>) responseObserver);
          break;
        case METHODID_GET_ALL_DATANODE:
          serviceImpl.getAllDatanode((mp.Metanode.GetAllDatanodeReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.GetAllDatanodeAck>) responseObserver);
          break;
        case METHODID_META_NODE_INFO:
          serviceImpl.metaNodeInfo((mp.Metanode.MetaNodeInfoReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.MetaNodeInfoAck>) responseObserver);
          break;
        case METHODID_VOLUME_INFOS:
          serviceImpl.volumeInfos((mp.Metanode.VolumeInfosReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.VolumeInfosAck>) responseObserver);
          break;
        case METHODID_GET_VOL_INFO:
          serviceImpl.getVolInfo((mp.Metanode.GetVolInfoReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.GetVolInfoAck>) responseObserver);
          break;
        case METHODID_NODE_MONITOR:
          serviceImpl.nodeMonitor((mp.Metanode.NodeMonitorReq) request,
              (io.grpc.stub.StreamObserver<mp.Metanode.NodeMonitorAck>) responseObserver);
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
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    return new io.grpc.ServiceDescriptor(SERVICE_NAME,
        METHOD_DATANODE_REGISTRY,
        METHOD_DEL_DATANODE,
        METHOD_CREATE_VOL,
        METHOD_EXPAND_VOL_TS,
        METHOD_EXPAND_VOL_RS,
        METHOD_DEL_VOL_RSFOR_EXPAND,
        METHOD_DELETE_VOL,
        METHOD_MIGRATE,
        METHOD_GET_META_LEADER,
        METHOD_CREATE_NAME_SPACE,
        METHOD_EXPAND_NAME_SPACE,
        METHOD_SNAP_SHOOT_NAME_SPACE,
        METHOD_DELETE_NAME_SPACE,
        METHOD_GET_FSINFO,
        METHOD_CREATE_DIR_DIRECT,
        METHOD_STAT_DIRECT,
        METHOD_GET_INODE_INFO_DIRECT,
        METHOD_LIST_DIRECT,
        METHOD_DELETE_DIR_DIRECT,
        METHOD_RENAME_DIRECT,
        METHOD_CREATE_FILE_DIRECT,
        METHOD_DELETE_FILE_DIRECT,
        METHOD_GET_FILE_CHUNKS_DIRECT,
        METHOD_ALLOCATE_CHUNK,
        METHOD_SYNC_CHUNK,
        METHOD_CLUSTER_INFO,
        METHOD_GET_ALL_DATANODE,
        METHOD_META_NODE_INFO,
        METHOD_VOLUME_INFOS,
        METHOD_GET_VOL_INFO,
        METHOD_NODE_MONITOR);
  }

}
