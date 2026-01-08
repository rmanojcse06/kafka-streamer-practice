## Step(1): Generate protobuf from my-brand-service.proto
```bash
mvn clean package
``` 
#### Or install protobuf in your laptop
```bash
    curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v25.5/protoc-25.5-osx-aarch_64.zip
    unzip protoc-25.5-osx-aarch_64.zip -d protoc_25
    sudo mv protoc_25/bin/protoc /usr/local/bin/protoc
    sudo cp -R protoc_25/include/google /usr/local/include/
    protoc --version
    # libprotoc 25.5
    sudo curl -L https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/1.66.0/protoc-gen-grpc-java-1.66.0-osx-aarch_64.exe -o /usr/local/bin/protoc-gen-grpc-java
    sudo chmod +x /usr/local/bin/protoc-gen-grpc-java
    protoc --proto_path=src/main/proto --java_out=target/generated-sources --grpc-java_out=target/generated-sources my-brand-service.proto
```

Now, We are going to transmit RPC using GRPC server
``` protobuf
rpc ProcessBrands(stream BrandStreamRequest) returns (stream BrandStreamResponse);
```

## Step(2): Start BrandStreamServer
```bash
cd simple-grpc-protobuf-service 
mvn clean install
mvn exec:java -Dexec.mainClass=edu.man.grpc.server.BrandStreamServer
```

## Step(3): Start BrandStreamClient
```bash
cd simple-grpc-protobuf-service
mvn clean install 
mvn exec:java -Dexec.mainClass=edu.man.grpc.client.BrandStreamClient
```
