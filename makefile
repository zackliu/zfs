include depends.mk

makeproto: file.pb.cc status_code.pb.cc nameserver.pb.cc
	echo "OK"

file.pb.cc: src/proto/file.proto
	$(PROTOC) --proto_path=./src/proto/ --proto_path=/usr/local/include --cpp_out=./src/proto/ src/proto/file.proto

status_code.pb.cc: src/proto/status_code.proto
	$(PROTOC) --proto_path=./src/proto/ --proto_path=/usr/local/include --cpp_out=./src/proto/ src/proto/status_code.proto

nameserver.pb.cc: src/proto/nameserver.proto
	$(PROTOC) --proto_path=./src/proto/ --proto_path=/usr/local/include --cpp_out=./src/proto/ src/proto/nameserver.proto