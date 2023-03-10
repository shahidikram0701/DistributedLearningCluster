// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.5
// source: scheduler.proto

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

// SchedulerServiceClient is the client API for SchedulerService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type SchedulerServiceClient interface {
	DeployModel(ctx context.Context, in *DeployModelRequest, opts ...grpc.CallOption) (*DeployModelReply, error)
	DeployModelAck(ctx context.Context, in *DeployModelAckRequest, opts ...grpc.CallOption) (*DeployModelAckReply, error)
	SubmitTask(ctx context.Context, in *SubmitTaskRequest, opts ...grpc.CallOption) (*SubmitTaskResponse, error)
	GetAllTasks(ctx context.Context, in *GetAllTasksRequest, opts ...grpc.CallOption) (*GetAllTasksResponse, error)
	GetAllTasksOfModel(ctx context.Context, in *GetAllTasksOfModelRequest, opts ...grpc.CallOption) (*GetAllTasksOfModelResponse, error)
	GetAllQueryRates(ctx context.Context, in *GetAllQueryRatesRequest, opts ...grpc.CallOption) (*GetAllQueryRatesResponse, error)
	GetQueryCount(ctx context.Context, in *GetQueryCountRequest, opts ...grpc.CallOption) (*GetQueryCountResponse, error)
	GetWorkersOfModel(ctx context.Context, in *GetWorkersOfModelRequest, opts ...grpc.CallOption) (*GetWorkersOfModelResponse, error)
	GetQueryAverageExectionTimes(ctx context.Context, in *GetQueryAverageExectionTimeRequest, opts ...grpc.CallOption) (*GetQueryAverageExectionTimeResponse, error)
	GimmeQuery(ctx context.Context, in *GimmeQueryRequest, opts ...grpc.CallOption) (*GimmeQueryResponse, error)
	UpdateQueryStatus(ctx context.Context, in *UpdateQueryStatusRequest, opts ...grpc.CallOption) (*UpdateQueryStatusResponse, error)
	GimmeModels(ctx context.Context, in *GimmeModelsRequest, opts ...grpc.CallOption) (*GimmeModelsResponse, error)
	SchedulerSync(ctx context.Context, in *SchedulerSyncRequest, opts ...grpc.CallOption) (*SchedulerSyncResponse, error)
}

type schedulerServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewSchedulerServiceClient(cc grpc.ClientConnInterface) SchedulerServiceClient {
	return &schedulerServiceClient{cc}
}

func (c *schedulerServiceClient) DeployModel(ctx context.Context, in *DeployModelRequest, opts ...grpc.CallOption) (*DeployModelReply, error) {
	out := new(DeployModelReply)
	err := c.cc.Invoke(ctx, "/process.SchedulerService/DeployModel", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerServiceClient) DeployModelAck(ctx context.Context, in *DeployModelAckRequest, opts ...grpc.CallOption) (*DeployModelAckReply, error) {
	out := new(DeployModelAckReply)
	err := c.cc.Invoke(ctx, "/process.SchedulerService/DeployModelAck", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerServiceClient) SubmitTask(ctx context.Context, in *SubmitTaskRequest, opts ...grpc.CallOption) (*SubmitTaskResponse, error) {
	out := new(SubmitTaskResponse)
	err := c.cc.Invoke(ctx, "/process.SchedulerService/SubmitTask", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerServiceClient) GetAllTasks(ctx context.Context, in *GetAllTasksRequest, opts ...grpc.CallOption) (*GetAllTasksResponse, error) {
	out := new(GetAllTasksResponse)
	err := c.cc.Invoke(ctx, "/process.SchedulerService/GetAllTasks", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerServiceClient) GetAllTasksOfModel(ctx context.Context, in *GetAllTasksOfModelRequest, opts ...grpc.CallOption) (*GetAllTasksOfModelResponse, error) {
	out := new(GetAllTasksOfModelResponse)
	err := c.cc.Invoke(ctx, "/process.SchedulerService/GetAllTasksOfModel", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerServiceClient) GetAllQueryRates(ctx context.Context, in *GetAllQueryRatesRequest, opts ...grpc.CallOption) (*GetAllQueryRatesResponse, error) {
	out := new(GetAllQueryRatesResponse)
	err := c.cc.Invoke(ctx, "/process.SchedulerService/GetAllQueryRates", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerServiceClient) GetQueryCount(ctx context.Context, in *GetQueryCountRequest, opts ...grpc.CallOption) (*GetQueryCountResponse, error) {
	out := new(GetQueryCountResponse)
	err := c.cc.Invoke(ctx, "/process.SchedulerService/GetQueryCount", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerServiceClient) GetWorkersOfModel(ctx context.Context, in *GetWorkersOfModelRequest, opts ...grpc.CallOption) (*GetWorkersOfModelResponse, error) {
	out := new(GetWorkersOfModelResponse)
	err := c.cc.Invoke(ctx, "/process.SchedulerService/GetWorkersOfModel", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerServiceClient) GetQueryAverageExectionTimes(ctx context.Context, in *GetQueryAverageExectionTimeRequest, opts ...grpc.CallOption) (*GetQueryAverageExectionTimeResponse, error) {
	out := new(GetQueryAverageExectionTimeResponse)
	err := c.cc.Invoke(ctx, "/process.SchedulerService/GetQueryAverageExectionTimes", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerServiceClient) GimmeQuery(ctx context.Context, in *GimmeQueryRequest, opts ...grpc.CallOption) (*GimmeQueryResponse, error) {
	out := new(GimmeQueryResponse)
	err := c.cc.Invoke(ctx, "/process.SchedulerService/GimmeQuery", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerServiceClient) UpdateQueryStatus(ctx context.Context, in *UpdateQueryStatusRequest, opts ...grpc.CallOption) (*UpdateQueryStatusResponse, error) {
	out := new(UpdateQueryStatusResponse)
	err := c.cc.Invoke(ctx, "/process.SchedulerService/UpdateQueryStatus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerServiceClient) GimmeModels(ctx context.Context, in *GimmeModelsRequest, opts ...grpc.CallOption) (*GimmeModelsResponse, error) {
	out := new(GimmeModelsResponse)
	err := c.cc.Invoke(ctx, "/process.SchedulerService/GimmeModels", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerServiceClient) SchedulerSync(ctx context.Context, in *SchedulerSyncRequest, opts ...grpc.CallOption) (*SchedulerSyncResponse, error) {
	out := new(SchedulerSyncResponse)
	err := c.cc.Invoke(ctx, "/process.SchedulerService/SchedulerSync", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SchedulerServiceServer is the server API for SchedulerService service.
// All implementations must embed UnimplementedSchedulerServiceServer
// for forward compatibility
type SchedulerServiceServer interface {
	DeployModel(context.Context, *DeployModelRequest) (*DeployModelReply, error)
	DeployModelAck(context.Context, *DeployModelAckRequest) (*DeployModelAckReply, error)
	SubmitTask(context.Context, *SubmitTaskRequest) (*SubmitTaskResponse, error)
	GetAllTasks(context.Context, *GetAllTasksRequest) (*GetAllTasksResponse, error)
	GetAllTasksOfModel(context.Context, *GetAllTasksOfModelRequest) (*GetAllTasksOfModelResponse, error)
	GetAllQueryRates(context.Context, *GetAllQueryRatesRequest) (*GetAllQueryRatesResponse, error)
	GetQueryCount(context.Context, *GetQueryCountRequest) (*GetQueryCountResponse, error)
	GetWorkersOfModel(context.Context, *GetWorkersOfModelRequest) (*GetWorkersOfModelResponse, error)
	GetQueryAverageExectionTimes(context.Context, *GetQueryAverageExectionTimeRequest) (*GetQueryAverageExectionTimeResponse, error)
	GimmeQuery(context.Context, *GimmeQueryRequest) (*GimmeQueryResponse, error)
	UpdateQueryStatus(context.Context, *UpdateQueryStatusRequest) (*UpdateQueryStatusResponse, error)
	GimmeModels(context.Context, *GimmeModelsRequest) (*GimmeModelsResponse, error)
	SchedulerSync(context.Context, *SchedulerSyncRequest) (*SchedulerSyncResponse, error)
	mustEmbedUnimplementedSchedulerServiceServer()
}

// UnimplementedSchedulerServiceServer must be embedded to have forward compatible implementations.
type UnimplementedSchedulerServiceServer struct {
}

func (UnimplementedSchedulerServiceServer) DeployModel(context.Context, *DeployModelRequest) (*DeployModelReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeployModel not implemented")
}
func (UnimplementedSchedulerServiceServer) DeployModelAck(context.Context, *DeployModelAckRequest) (*DeployModelAckReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeployModelAck not implemented")
}
func (UnimplementedSchedulerServiceServer) SubmitTask(context.Context, *SubmitTaskRequest) (*SubmitTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SubmitTask not implemented")
}
func (UnimplementedSchedulerServiceServer) GetAllTasks(context.Context, *GetAllTasksRequest) (*GetAllTasksResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAllTasks not implemented")
}
func (UnimplementedSchedulerServiceServer) GetAllTasksOfModel(context.Context, *GetAllTasksOfModelRequest) (*GetAllTasksOfModelResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAllTasksOfModel not implemented")
}
func (UnimplementedSchedulerServiceServer) GetAllQueryRates(context.Context, *GetAllQueryRatesRequest) (*GetAllQueryRatesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAllQueryRates not implemented")
}
func (UnimplementedSchedulerServiceServer) GetQueryCount(context.Context, *GetQueryCountRequest) (*GetQueryCountResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetQueryCount not implemented")
}
func (UnimplementedSchedulerServiceServer) GetWorkersOfModel(context.Context, *GetWorkersOfModelRequest) (*GetWorkersOfModelResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetWorkersOfModel not implemented")
}
func (UnimplementedSchedulerServiceServer) GetQueryAverageExectionTimes(context.Context, *GetQueryAverageExectionTimeRequest) (*GetQueryAverageExectionTimeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetQueryAverageExectionTimes not implemented")
}
func (UnimplementedSchedulerServiceServer) GimmeQuery(context.Context, *GimmeQueryRequest) (*GimmeQueryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GimmeQuery not implemented")
}
func (UnimplementedSchedulerServiceServer) UpdateQueryStatus(context.Context, *UpdateQueryStatusRequest) (*UpdateQueryStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateQueryStatus not implemented")
}
func (UnimplementedSchedulerServiceServer) GimmeModels(context.Context, *GimmeModelsRequest) (*GimmeModelsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GimmeModels not implemented")
}
func (UnimplementedSchedulerServiceServer) SchedulerSync(context.Context, *SchedulerSyncRequest) (*SchedulerSyncResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SchedulerSync not implemented")
}
func (UnimplementedSchedulerServiceServer) mustEmbedUnimplementedSchedulerServiceServer() {}

// UnsafeSchedulerServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to SchedulerServiceServer will
// result in compilation errors.
type UnsafeSchedulerServiceServer interface {
	mustEmbedUnimplementedSchedulerServiceServer()
}

func RegisterSchedulerServiceServer(s grpc.ServiceRegistrar, srv SchedulerServiceServer) {
	s.RegisterService(&SchedulerService_ServiceDesc, srv)
}

func _SchedulerService_DeployModel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeployModelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServiceServer).DeployModel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.SchedulerService/DeployModel",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServiceServer).DeployModel(ctx, req.(*DeployModelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerService_DeployModelAck_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeployModelAckRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServiceServer).DeployModelAck(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.SchedulerService/DeployModelAck",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServiceServer).DeployModelAck(ctx, req.(*DeployModelAckRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerService_SubmitTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SubmitTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServiceServer).SubmitTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.SchedulerService/SubmitTask",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServiceServer).SubmitTask(ctx, req.(*SubmitTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerService_GetAllTasks_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetAllTasksRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServiceServer).GetAllTasks(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.SchedulerService/GetAllTasks",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServiceServer).GetAllTasks(ctx, req.(*GetAllTasksRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerService_GetAllTasksOfModel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetAllTasksOfModelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServiceServer).GetAllTasksOfModel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.SchedulerService/GetAllTasksOfModel",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServiceServer).GetAllTasksOfModel(ctx, req.(*GetAllTasksOfModelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerService_GetAllQueryRates_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetAllQueryRatesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServiceServer).GetAllQueryRates(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.SchedulerService/GetAllQueryRates",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServiceServer).GetAllQueryRates(ctx, req.(*GetAllQueryRatesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerService_GetQueryCount_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetQueryCountRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServiceServer).GetQueryCount(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.SchedulerService/GetQueryCount",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServiceServer).GetQueryCount(ctx, req.(*GetQueryCountRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerService_GetWorkersOfModel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetWorkersOfModelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServiceServer).GetWorkersOfModel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.SchedulerService/GetWorkersOfModel",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServiceServer).GetWorkersOfModel(ctx, req.(*GetWorkersOfModelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerService_GetQueryAverageExectionTimes_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetQueryAverageExectionTimeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServiceServer).GetQueryAverageExectionTimes(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.SchedulerService/GetQueryAverageExectionTimes",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServiceServer).GetQueryAverageExectionTimes(ctx, req.(*GetQueryAverageExectionTimeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerService_GimmeQuery_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GimmeQueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServiceServer).GimmeQuery(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.SchedulerService/GimmeQuery",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServiceServer).GimmeQuery(ctx, req.(*GimmeQueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerService_UpdateQueryStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateQueryStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServiceServer).UpdateQueryStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.SchedulerService/UpdateQueryStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServiceServer).UpdateQueryStatus(ctx, req.(*UpdateQueryStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerService_GimmeModels_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GimmeModelsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServiceServer).GimmeModels(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.SchedulerService/GimmeModels",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServiceServer).GimmeModels(ctx, req.(*GimmeModelsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerService_SchedulerSync_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SchedulerSyncRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerServiceServer).SchedulerSync(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/process.SchedulerService/SchedulerSync",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerServiceServer).SchedulerSync(ctx, req.(*SchedulerSyncRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// SchedulerService_ServiceDesc is the grpc.ServiceDesc for SchedulerService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var SchedulerService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "process.SchedulerService",
	HandlerType: (*SchedulerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "DeployModel",
			Handler:    _SchedulerService_DeployModel_Handler,
		},
		{
			MethodName: "DeployModelAck",
			Handler:    _SchedulerService_DeployModelAck_Handler,
		},
		{
			MethodName: "SubmitTask",
			Handler:    _SchedulerService_SubmitTask_Handler,
		},
		{
			MethodName: "GetAllTasks",
			Handler:    _SchedulerService_GetAllTasks_Handler,
		},
		{
			MethodName: "GetAllTasksOfModel",
			Handler:    _SchedulerService_GetAllTasksOfModel_Handler,
		},
		{
			MethodName: "GetAllQueryRates",
			Handler:    _SchedulerService_GetAllQueryRates_Handler,
		},
		{
			MethodName: "GetQueryCount",
			Handler:    _SchedulerService_GetQueryCount_Handler,
		},
		{
			MethodName: "GetWorkersOfModel",
			Handler:    _SchedulerService_GetWorkersOfModel_Handler,
		},
		{
			MethodName: "GetQueryAverageExectionTimes",
			Handler:    _SchedulerService_GetQueryAverageExectionTimes_Handler,
		},
		{
			MethodName: "GimmeQuery",
			Handler:    _SchedulerService_GimmeQuery_Handler,
		},
		{
			MethodName: "UpdateQueryStatus",
			Handler:    _SchedulerService_UpdateQueryStatus_Handler,
		},
		{
			MethodName: "GimmeModels",
			Handler:    _SchedulerService_GimmeModels_Handler,
		},
		{
			MethodName: "SchedulerSync",
			Handler:    _SchedulerService_SchedulerSync_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "scheduler.proto",
}
