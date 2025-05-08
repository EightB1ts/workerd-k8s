// Copyright (c) 2025 Cameron Halter
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/io/container.capnp.h>
#include <workerd/jsg/jsg.h>
#include <kj/async-io.h>
#include <kj/compat/http.h>

namespace workerd::server {

// Define byte type since we removed jsg.h
using byte = unsigned char;

// Container service implementation that interacts with Kubernetes
class KubernetesContainerService final: public rpc::Container::Server {
public:
  KubernetesContainerService(
      kj::StringPtr actorId,
      kj::StringPtr namespace_,
      kj::StringPtr apiServer,
      kj::StringPtr serviceAccount,
      kj::StringPtr image,
      uint32_t memoryLimitMb = 32768,
      uint32_t cpuLimitMillicores = 8000,
      bool runAsNonRoot = true,
      kj::StringPtr serviceAccountToken = "");

  // Create a container service with a pre-configured HTTP client
  KubernetesContainerService(
      kj::StringPtr actorId,
      kj::StringPtr namespace_,
      kj::StringPtr apiServer,
      kj::StringPtr serviceAccount,
      kj::StringPtr image,
      kj::HttpClient& httpClient,
      uint32_t memoryLimitMb = 32768,
      uint32_t cpuLimitMillicores = 8000,
      bool runAsNonRoot = true,
      kj::StringPtr serviceAccountToken = "");

  ~KubernetesContainerService() noexcept(false);

  kj::Promise<void> status(StatusContext context) override;
  kj::Promise<void> start(StartContext context) override;
  kj::Promise<void> monitor(MonitorContext context) override;
  kj::Promise<void> destroy(DestroyContext context) override;
  kj::Promise<void> signal(SignalContext context) override;
  kj::Promise<void> getTcpPort(GetTcpPortContext context) override;
  kj::Promise<void> listenTcp(ListenTcpContext context) override;

private:
  kj::String actorId;
  kj::String namespace_;
  kj::String apiServer;
  kj::String serviceAccount;
  kj::String image;
  uint32_t memoryLimitMb;
  uint32_t cpuLimitMillicores;
  bool runAsNonRoot;
  bool running = false;
  kj::Maybe<kj::Own<kj::PromiseFulfiller<void>>> monitorFulfiller;
  kj::Maybe<kj::HttpClient&> httpClient;
  kj::String serviceAccountToken;

  kj::Maybe<kj::HttpClient&> getHttpClient();
  kj::String getPodName();
  kj::Own<kj::HttpHeaders> createKubernetesHeaders();
  kj::String buildPodSpec(kj::ArrayPtr<kj::String> entrypoint,
                          kj::ArrayPtr<kj::String> envVars,
                          bool enableInternet);

  class KubernetesPortImpl;
  class ByteStreamAdapter;

  // Port implementation
  class KubernetesPortImpl final: public rpc::Container::Port::Server {
  public:
    KubernetesPortImpl(kj::StringPtr podName, kj::StringPtr namespace_,
                     uint16_t port, kj::Maybe<kj::HttpClient&> httpClient, kj::StringPtr apiServer);

    kj::Promise<void> connect(ConnectContext context) override;

    static kj::Promise<void> pumpDataFromClient(capnp::ByteStream::Client downstream, kj::Own<ByteStreamAdapter> adapter);

  private:
    kj::String podName;
    kj::String namespace_;
    uint16_t port;
    kj::Maybe<kj::HttpClient&> httpClient;
    kj::String apiServer;

    // ByteStream adapter that implements the capnp::ByteStream::Server interface
    class ByteStreamAdapter: public capnp::ByteStream::Server, public kj::Refcounted {
    public:
      ByteStreamAdapter();
      void setConnection(kj::AsyncInputStream* inputStream);
      void setOutputStream(kj::AsyncOutputStream* outputStream);
      kj::Promise<void> registerDataCallback(kj::Promise<void> awaitData);
      kj::Promise<kj::Maybe<kj::Array<byte>>> readNextData();
      virtual ~ByteStreamAdapter() noexcept(false);
      kj::Promise<void> write(WriteContext context) override;
      kj::Promise<void> end(EndContext context) override;

    private:
      bool active;
      kj::AsyncInputStream* connection = nullptr;
      kj::Maybe<kj::AsyncInputStream*> inputConnection;
      kj::Maybe<kj::AsyncOutputStream*> outputConnection;
      kj::Maybe<kj::Own<kj::PromiseFulfiller<void>>> readCompleteFulfiller;
      kj::Vector<kj::Own<kj::PromiseFulfiller<void>>> streamDataFulfillers;
      kj::Vector<kj::Own<kj::PromiseFulfiller<kj::Maybe<kj::Array<byte>>>>> dataFulfillers;

      void startReading();
      kj::Promise<void> readHeader(kj::AsyncInputStream* input, kj::Array<byte> headerBuffer);
      kj::Promise<void> processHeader(kj::AsyncInputStream* input, kj::Array<byte> headerBuffer);
      kj::Promise<void> readPayload(kj::AsyncInputStream* input, kj::Array<byte> payloadBuffer,
                                  byte channel, uint32_t size);
      void processPayload(byte channel, kj::Array<byte> payload);
      void forwardStreamData(kj::Array<byte> data, bool isStdout);
    };

    static kj::Promise<void> pumpDataFromClient(capnp::ByteStream::Client downstream, kj::Own<ByteStreamAdapter> adapter);

    // A simple implementation of ByteStream that immediately ends
    class EmptyByteStream final: public capnp::ByteStream::Server {
    public:
      kj::Promise<void> write(WriteContext context) override;
      kj::Promise<void> end(EndContext context) override;
    };
  };
};

} // namespace workerd::server
