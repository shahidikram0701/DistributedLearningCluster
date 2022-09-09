// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.5
// source: logger.proto

package logger

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// The request message containing the user's name.
type FindLogsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Query string `protobuf:"bytes,1,opt,name=query,proto3" json:"query,omitempty"`
}

func (x *FindLogsRequest) Reset() {
	*x = FindLogsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_logger_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FindLogsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FindLogsRequest) ProtoMessage() {}

func (x *FindLogsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_logger_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FindLogsRequest.ProtoReflect.Descriptor instead.
func (*FindLogsRequest) Descriptor() ([]byte, []int) {
	return file_logger_proto_rawDescGZIP(), []int{0}
}

func (x *FindLogsRequest) GetQuery() string {
	if x != nil {
		return x.Query
	}
	return ""
}

// The response message containing the greetings
type FindLogsReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Logs    string `protobuf:"bytes,1,opt,name=logs,proto3" json:"logs,omitempty"`
	Matches int32  `protobuf:"varint,2,opt,name=matches,proto3" json:"matches,omitempty"`
}

func (x *FindLogsReply) Reset() {
	*x = FindLogsReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_logger_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FindLogsReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FindLogsReply) ProtoMessage() {}

func (x *FindLogsReply) ProtoReflect() protoreflect.Message {
	mi := &file_logger_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FindLogsReply.ProtoReflect.Descriptor instead.
func (*FindLogsReply) Descriptor() ([]byte, []int) {
	return file_logger_proto_rawDescGZIP(), []int{1}
}

func (x *FindLogsReply) GetLogs() string {
	if x != nil {
		return x.Logs
	}
	return ""
}

func (x *FindLogsReply) GetMatches() int32 {
	if x != nil {
		return x.Matches
	}
	return 0
}

type GenerateLogsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Filenumber int32 `protobuf:"varint,1,opt,name=filenumber,proto3" json:"filenumber,omitempty"`
}

func (x *GenerateLogsRequest) Reset() {
	*x = GenerateLogsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_logger_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GenerateLogsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GenerateLogsRequest) ProtoMessage() {}

func (x *GenerateLogsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_logger_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GenerateLogsRequest.ProtoReflect.Descriptor instead.
func (*GenerateLogsRequest) Descriptor() ([]byte, []int) {
	return file_logger_proto_rawDescGZIP(), []int{2}
}

func (x *GenerateLogsRequest) GetFilenumber() int32 {
	if x != nil {
		return x.Filenumber
	}
	return 0
}

type GenerateLogsReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Status string `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
}

func (x *GenerateLogsReply) Reset() {
	*x = GenerateLogsReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_logger_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GenerateLogsReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GenerateLogsReply) ProtoMessage() {}

func (x *GenerateLogsReply) ProtoReflect() protoreflect.Message {
	mi := &file_logger_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GenerateLogsReply.ProtoReflect.Descriptor instead.
func (*GenerateLogsReply) Descriptor() ([]byte, []int) {
	return file_logger_proto_rawDescGZIP(), []int{3}
}

func (x *GenerateLogsReply) GetStatus() string {
	if x != nil {
		return x.Status
	}
	return ""
}

var File_logger_proto protoreflect.FileDescriptor

var file_logger_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x6c, 0x6f, 0x67, 0x67, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06,
	0x6c, 0x6f, 0x67, 0x67, 0x65, 0x72, 0x22, 0x27, 0x0a, 0x0f, 0x46, 0x69, 0x6e, 0x64, 0x4c, 0x6f,
	0x67, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x71, 0x75, 0x65,
	0x72, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x71, 0x75, 0x65, 0x72, 0x79, 0x22,
	0x3d, 0x0a, 0x0d, 0x46, 0x69, 0x6e, 0x64, 0x4c, 0x6f, 0x67, 0x73, 0x52, 0x65, 0x70, 0x6c, 0x79,
	0x12, 0x12, 0x0a, 0x04, 0x6c, 0x6f, 0x67, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x6c, 0x6f, 0x67, 0x73, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x65, 0x73, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x07, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x65, 0x73, 0x22, 0x35,
	0x0a, 0x13, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x4c, 0x6f, 0x67, 0x73, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1e, 0x0a, 0x0a, 0x66, 0x69, 0x6c, 0x65, 0x6e, 0x75, 0x6d,
	0x62, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0a, 0x66, 0x69, 0x6c, 0x65, 0x6e,
	0x75, 0x6d, 0x62, 0x65, 0x72, 0x22, 0x2b, 0x0a, 0x11, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74,
	0x65, 0x4c, 0x6f, 0x67, 0x73, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x74,
	0x61, 0x74, 0x75, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74,
	0x75, 0x73, 0x32, 0x95, 0x01, 0x0a, 0x06, 0x4c, 0x6f, 0x67, 0x67, 0x65, 0x72, 0x12, 0x3c, 0x0a,
	0x08, 0x46, 0x69, 0x6e, 0x64, 0x4c, 0x6f, 0x67, 0x73, 0x12, 0x17, 0x2e, 0x6c, 0x6f, 0x67, 0x67,
	0x65, 0x72, 0x2e, 0x46, 0x69, 0x6e, 0x64, 0x4c, 0x6f, 0x67, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x15, 0x2e, 0x6c, 0x6f, 0x67, 0x67, 0x65, 0x72, 0x2e, 0x46, 0x69, 0x6e, 0x64,
	0x4c, 0x6f, 0x67, 0x73, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x00, 0x12, 0x4d, 0x0a, 0x11, 0x54,
	0x65, 0x73, 0x74, 0x5f, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x4c, 0x6f, 0x67, 0x73,
	0x12, 0x1b, 0x2e, 0x6c, 0x6f, 0x67, 0x67, 0x65, 0x72, 0x2e, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x61,
	0x74, 0x65, 0x4c, 0x6f, 0x67, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x19, 0x2e,
	0x6c, 0x6f, 0x67, 0x67, 0x65, 0x72, 0x2e, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x4c,
	0x6f, 0x67, 0x73, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x00, 0x42, 0x12, 0x5a, 0x10, 0x63, 0x73,
	0x34, 0x32, 0x35, 0x2f, 0x6d, 0x70, 0x31, 0x2f, 0x6c, 0x6f, 0x67, 0x67, 0x65, 0x72, 0x62, 0x06,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_logger_proto_rawDescOnce sync.Once
	file_logger_proto_rawDescData = file_logger_proto_rawDesc
)

func file_logger_proto_rawDescGZIP() []byte {
	file_logger_proto_rawDescOnce.Do(func() {
		file_logger_proto_rawDescData = protoimpl.X.CompressGZIP(file_logger_proto_rawDescData)
	})
	return file_logger_proto_rawDescData
}

var file_logger_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_logger_proto_goTypes = []interface{}{
	(*FindLogsRequest)(nil),     // 0: logger.FindLogsRequest
	(*FindLogsReply)(nil),       // 1: logger.FindLogsReply
	(*GenerateLogsRequest)(nil), // 2: logger.GenerateLogsRequest
	(*GenerateLogsReply)(nil),   // 3: logger.GenerateLogsReply
}
var file_logger_proto_depIdxs = []int32{
	0, // 0: logger.Logger.FindLogs:input_type -> logger.FindLogsRequest
	2, // 1: logger.Logger.Test_GenerateLogs:input_type -> logger.GenerateLogsRequest
	1, // 2: logger.Logger.FindLogs:output_type -> logger.FindLogsReply
	3, // 3: logger.Logger.Test_GenerateLogs:output_type -> logger.GenerateLogsReply
	2, // [2:4] is the sub-list for method output_type
	0, // [0:2] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_logger_proto_init() }
func file_logger_proto_init() {
	if File_logger_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_logger_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FindLogsRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_logger_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FindLogsReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_logger_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GenerateLogsRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_logger_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GenerateLogsReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_logger_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_logger_proto_goTypes,
		DependencyIndexes: file_logger_proto_depIdxs,
		MessageInfos:      file_logger_proto_msgTypes,
	}.Build()
	File_logger_proto = out.File
	file_logger_proto_rawDesc = nil
	file_logger_proto_goTypes = nil
	file_logger_proto_depIdxs = nil
}
