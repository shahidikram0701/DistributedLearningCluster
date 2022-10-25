// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.5
// source: data_node_proto.proto

package process

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// DataNodeServiceClient is the client API for DataNodeService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DataNodeServiceClient interface {
	DataNode_PutFile(ctx context.Context, opts ...grpc.CallOption) (DataNodeService_DataNode_PutFileClient, error)
	DataNode_CommitFile(ctx context.Context, in *DataNode_CommitFileRequest, opts ...grpc.CallOption) (*DataNode_CommitFileResponse, error)
}

type dataNodeServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewDataNodeServiceClient(cc grpc.ClientConnInterface) DataNodeServiceClient {
	return &dataNodeServiceClient{cc}
}

func (c *dataNodeServiceClient) DataNode_PutFile(ctx context.Context, opts ...grpc.CallOption) (DataNodeService_DataNode_PutFileClient, error) {
	stream, err := c.cc.NewStream(ctx, &DataNodeService_ServiceDesc.Streams[0], "/process.DataNodeService/DataNode_PutFile", opts...)
	if err != nil {
		return nil, err
	}
	x := &dataNodeServiceDataNode_PutFileClient{stream}
	return x, nil
}

type DataNodeService_DataNode_PutFileClient interface {
	Send(*Chunk) error
	CloseAndRecv() (*DataNode_PutFile_Response, error)
	grpc.ClientStream
}

type dataNodeServiceDataNode_PutFileClient struct {
	grpc.ClientStream
}

func (x *dataNodeServiceDataNode_PutFileClient) Send(m *Chunk) error {
	return x.ClientStream.SendMsg(m)
}

func (x *dataNodeServiceDataNode_PutFileClient) CloseAndRecv() (*DataNode_PutFile_Response, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(DataNode_PutFile_Response)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *dataNodeServiceClient) DataNode_CommitFile(ctx context.Context, in *DataNode_CommitFileRequest, opts ...grpc.CallOption) (*DataNode_CommitFileResponse, error) {
	out := new(DataNode_CommitFileResponse)
	err := c.cc.Invoke(ctx, "/process.DataNodeService/DataNode_CommitFile", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DataNodeServiceServer is the server API for DataNodeService service.
// All implementations must embed UnimplementedDataNodeServiceServer
// for forward compatibility
type DataNodeServiceServer interface {
	DataNode_PutFile(DataNodeService_DataNode_PutFileServer) error
	DataNode_CommitFile(context.Context, *DataNode_CommitFileRequest) (*DataNode_CommitFileResponse, error)
	mustEmbedUnimplementedDataNodeServiceServer()
}

// UnimplementedDataNodeServiceServer must be embedded to have forward compatible implementations.
type UnimplementedDataNodeServiceServer struct {
}

func (UnimplementedDataNodeServiceServer) DataNode_PutFile(DataNodeService_DataNode_PutFileServer) error {
	return status.Errorf(codes.Unimplemented, "method DataNode_PutFile not implemented")
}
func (UnimplementedDataNodeServiceServer) DataNode_CommitFile(context.Context, *DataNode_CommitFileRequest) (*DataNode_CommitFileResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DataNode_CommitFile not implemented")
}
func (UnimplementedDataNodeServiceServer) mustEmbedUnimplementedDataNodeServiceServer() {}

// UnsafeDataNodeServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DataNodeServiceServer will
// result in compilation errors.
type UnsafeDataNodeServiceServer interface {
	mustEmbedUnimplementedDataNodeServiceServer()
}

func RegisterDataNodeServiceServer(s grpc.ServiceRegistrar, srv DataNodeServiceServer) {
	s.RegisterService(&DataNodeService_ServiceDesc, srv)
}

func _DataNodeService_DataNode_PutFile_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(DataNodeServiceServer).DataNode_PutFile(&dataNodeServiceDataNode_PutFileServer{stream})
}

type DataNodeService_DataNode_PutFileServer interface {
	SendAndClose(*DataNode_PutFile_Response) error
	Recv() (*Chunk, error)
	grpc.ServerStream
}

type dataNodeServiceDataNode_PutFileServer struct {
	grpc.ServerStream
}

func (x *dataNodeServiceDataNode_PutFileServer) SendAndClose(m *DataNode_PutFile_Response) error {
	return x.ServerStream.SendMsg(m)
}

func (x *dataNodeServiceDataNode_PutFileServer) Recv() (*Chunk, error) {
	m := new(Chunk)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _DataNodeService_DataNode_CommitFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DataNode_CommitFileRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DataNodeServiceServer).DataNode_CommitFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.DataNodeService/DataNode_CommitFile",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DataNodeServiceServer).DataNode_CommitFile(ctx, req.(*DataNode_CommitFileRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// DataNodeService_ServiceDesc is the grpc.ServiceDesc for DataNodeService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var DataNodeService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "process.DataNodeService",
	HandlerType: (*DataNodeServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "DataNode_CommitFile",
			Handler:    _DataNodeService_DataNode_CommitFile_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "DataNode_PutFile",
			Handler:       _DataNodeService_DataNode_PutFile_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "data_node_proto.proto",
}
