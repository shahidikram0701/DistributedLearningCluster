// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.5
// source: coordinator.proto

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

// CoordinatorServiceForLogsClient is the client API for CoordinatorServiceForLogs service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type CoordinatorServiceForLogsClient interface {
	QueryLogs(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (*QueryReply, error)
	Test_Coordinator_GenerateLogs(ctx context.Context, in *Test_Coordinator_GenerateLogsRequest, opts ...grpc.CallOption) (*Test_Coordinator_GenerateLogsReply, error)
}

type coordinatorServiceForLogsClient struct {
	cc grpc.ClientConnInterface
}

func NewCoordinatorServiceForLogsClient(cc grpc.ClientConnInterface) CoordinatorServiceForLogsClient {
	return &coordinatorServiceForLogsClient{cc}
}

func (c *coordinatorServiceForLogsClient) QueryLogs(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (*QueryReply, error) {
	out := new(QueryReply)
	err := c.cc.Invoke(ctx, "/process.CoordinatorServiceForLogs/QueryLogs", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *coordinatorServiceForLogsClient) Test_Coordinator_GenerateLogs(ctx context.Context, in *Test_Coordinator_GenerateLogsRequest, opts ...grpc.CallOption) (*Test_Coordinator_GenerateLogsReply, error) {
	out := new(Test_Coordinator_GenerateLogsReply)
	err := c.cc.Invoke(ctx, "/process.CoordinatorServiceForLogs/Test_Coordinator_GenerateLogs", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CoordinatorServiceForLogsServer is the server API for CoordinatorServiceForLogs service.
// All implementations must embed UnimplementedCoordinatorServiceForLogsServer
// for forward compatibility
type CoordinatorServiceForLogsServer interface {
	QueryLogs(context.Context, *QueryRequest) (*QueryReply, error)
	Test_Coordinator_GenerateLogs(context.Context, *Test_Coordinator_GenerateLogsRequest) (*Test_Coordinator_GenerateLogsReply, error)
	mustEmbedUnimplementedCoordinatorServiceForLogsServer()
}

// UnimplementedCoordinatorServiceForLogsServer must be embedded to have forward compatible implementations.
type UnimplementedCoordinatorServiceForLogsServer struct {
}

func (UnimplementedCoordinatorServiceForLogsServer) QueryLogs(context.Context, *QueryRequest) (*QueryReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method QueryLogs not implemented")
}
func (UnimplementedCoordinatorServiceForLogsServer) Test_Coordinator_GenerateLogs(context.Context, *Test_Coordinator_GenerateLogsRequest) (*Test_Coordinator_GenerateLogsReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Test_Coordinator_GenerateLogs not implemented")
}
func (UnimplementedCoordinatorServiceForLogsServer) mustEmbedUnimplementedCoordinatorServiceForLogsServer() {
}

// UnsafeCoordinatorServiceForLogsServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to CoordinatorServiceForLogsServer will
// result in compilation errors.
type UnsafeCoordinatorServiceForLogsServer interface {
	mustEmbedUnimplementedCoordinatorServiceForLogsServer()
}

func RegisterCoordinatorServiceForLogsServer(s grpc.ServiceRegistrar, srv CoordinatorServiceForLogsServer) {
	s.RegisterService(&CoordinatorServiceForLogs_ServiceDesc, srv)
}

func _CoordinatorServiceForLogs_QueryLogs_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(QueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CoordinatorServiceForLogsServer).QueryLogs(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.CoordinatorServiceForLogs/QueryLogs",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CoordinatorServiceForLogsServer).QueryLogs(ctx, req.(*QueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _CoordinatorServiceForLogs_Test_Coordinator_GenerateLogs_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Test_Coordinator_GenerateLogsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CoordinatorServiceForLogsServer).Test_Coordinator_GenerateLogs(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.CoordinatorServiceForLogs/Test_Coordinator_GenerateLogs",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CoordinatorServiceForLogsServer).Test_Coordinator_GenerateLogs(ctx, req.(*Test_Coordinator_GenerateLogsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// CoordinatorServiceForLogs_ServiceDesc is the grpc.ServiceDesc for CoordinatorServiceForLogs service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var CoordinatorServiceForLogs_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "process.CoordinatorServiceForLogs",
	HandlerType: (*CoordinatorServiceForLogsServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "QueryLogs",
			Handler:    _CoordinatorServiceForLogs_QueryLogs_Handler,
		},
		{
			MethodName: "Test_Coordinator_GenerateLogs",
			Handler:    _CoordinatorServiceForLogs_Test_Coordinator_GenerateLogs_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "coordinator.proto",
}
