// Copyright (c) 2025 Cameron Halter
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "container.h"

namespace workerd::server {

KubernetesContainerService::KubernetesContainerService(
    kj::StringPtr actorId,
    kj::StringPtr namespace_,
    kj::StringPtr apiServer,
    kj::StringPtr serviceAccount,
    kj::StringPtr image,
    uint32_t memoryLimitMb,
    uint32_t cpuLimitMillicores,
    bool runAsNonRoot,
    kj::StringPtr serviceAccountToken)
    : actorId(kj::heapString(actorId)),
      namespace_(kj::heapString(namespace_)),
      apiServer(kj::heapString(apiServer)),
      serviceAccount(kj::heapString(serviceAccount)),
      image(kj::heapString(image)),
      memoryLimitMb(memoryLimitMb),
      cpuLimitMillicores(cpuLimitMillicores),
      runAsNonRoot(runAsNonRoot),
      serviceAccountToken(kj::heapString(serviceAccountToken)) {}

KubernetesContainerService::KubernetesContainerService(
    kj::StringPtr actorId,
    kj::StringPtr namespace_,
    kj::StringPtr apiServer,
    kj::StringPtr serviceAccount,
    kj::StringPtr image,
    kj::HttpClient& httpClient,
    uint32_t memoryLimitMb,
    uint32_t cpuLimitMillicores,
    bool runAsNonRoot,
    kj::StringPtr serviceAccountToken)
    : KubernetesContainerService(actorId, namespace_, apiServer, serviceAccount, image,
                                memoryLimitMb, cpuLimitMillicores, runAsNonRoot, serviceAccountToken) {
  this->httpClient = &httpClient;
}

KubernetesContainerService::~KubernetesContainerService() noexcept(false) {
  // Ensure container is destroyed if service is destroyed
  if (running) {
    KJ_IF_SOME(client, getHttpClient()) {
      // Simplified approach - create URL and headers directly
      auto url = kj::str("https://", apiServer, "/api/v1/namespaces/", namespace_, "/pods/", getPodName());
      auto headers = createKubernetesHeaders();
      // Use the HTTP client with the proper number of args (method, url, headers)
      client.request(kj::HttpMethod::DELETE, url, *headers);
    }
  }
}

kj::Promise<void> KubernetesContainerService::status(StatusContext context) {
  // Query Kubernetes API to check if pod is running
  KJ_IF_SOME(client, getHttpClient()) {
    auto headers = createKubernetesHeaders();
    auto url = kj::str("https://", apiServer, "/api/v1/namespaces/", namespace_, "/pods/", getPodName());

    auto request = client.request(kj::HttpMethod::GET, url, *headers);

    return request.response.then(
        [this, &context](kj::HttpClient::Response&& response) mutable -> kj::Promise<void> {
      switch (response.statusCode) {
        case 200:
          return response.body->readAllBytes().then(
              [this, &context](kj::Array<byte> body) mutable -> kj::Promise<void> {
            // Convert body to string and parse JSON properly to check pod status
            auto bodyStr = kj::StringPtr((const char*)body.begin(), body.size());

            // In a production implementation, use a proper JSON parser
            // For now, we're checking for the phase field in the status
            if (bodyStr.find("\"phase\":\"Running\"") != kj::none) {
              running = true;
              context.getResults().setRunning(true);
            } else if (bodyStr.find("\"phase\":\"Pending\"") != kj::none) {
              // Pod exists but is not yet running
              running = false;
              context.getResults().setRunning(false);
            } else if (bodyStr.find("\"phase\":\"Succeeded\"") != kj::none ||
                      bodyStr.find("\"phase\":\"Failed\"") != kj::none) {
              // Pod has completed or failed
              running = false;
              context.getResults().setRunning(false);
            } else {
              // Unknown status
              running = false;
              context.getResults().setRunning(false);
            }
            return kj::READY_NOW;
          });

        case 404:
          // Pod not found
          running = false;
          context.getResults().setRunning(false);
          return kj::READY_NOW;

        default:
          return response.body->readAllBytes().then(
              [this, &context, &response](kj::Array<byte> body) mutable -> kj::Promise<void> {
            auto errorBody = kj::StringPtr((const char*)body.begin(), body.size());
            KJ_LOG(WARNING, "Error checking container status",
                  response.statusCode, errorBody);
            running = false;
            context.getResults().setRunning(false);
            return kj::READY_NOW;
          });
      }
    }).catch_([this, &context](kj::Exception&& e) mutable -> kj::Promise<void> {
      // Handle error cases
      KJ_LOG(WARNING, "Exception checking container status", e);
      running = false;
      context.getResults().setRunning(false);
      return kj::READY_NOW;
    });
  } else {
    // If we can't access Kubernetes, report as not running
    context.getResults().setRunning(false);
    return kj::READY_NOW;
  }
}

kj::Promise<void> KubernetesContainerService::start(StartContext context) {
  KJ_REQUIRE(!running, "Container is already running");

  KJ_IF_SOME(client, getHttpClient()) {
    auto params = context.getParams();

    // Build Kubernetes pod specification
    kj::Vector<kj::String> envVars;
    for (const auto& env : params.getEnvironmentVariables()) {
      envVars.add(kj::str(env));
    }

    kj::Vector<kj::String> entrypoint;
    for (const auto& cmd : params.getEntrypoint()) {
      entrypoint.add(kj::str(cmd));
    }

    auto podSpec = buildPodSpec(entrypoint, envVars, params.getEnableInternet());

    // Create pod in Kubernetes
    auto headers = createKubernetesHeaders();
    auto url = kj::str("https://", apiServer, "/api/v1/namespaces/", namespace_, "/pods");

    // Create a request with a body containing the pod spec
    auto request = client.request(kj::HttpMethod::POST, url, *headers, podSpec.size());
    auto writePromise = request.body->write(podSpec.asBytes());
    // Request response will implicitly wait for writePromise since the HTTP client handles this
    return request.response.then(
        [this](kj::HttpClient::Response&& response) -> kj::Promise<void> {
      if (response.statusCode == 201) {
        running = true;
        // TODO: Start monitoring the pod for status changes
        return kj::READY_NOW;
      } else {
        return response.body->readAllBytes().then(
            [&response](kj::Array<byte> body) -> kj::Promise<void> {
          KJ_LOG(WARNING, "Failed to create Kubernetes pod",
            response.statusCode, kj::StringPtr((const char*)body.begin(), body.size()));
          return kj::READY_NOW;
        });
      }
    }).catch_([](kj::Exception&& e) -> kj::Promise<void> {
      KJ_LOG(WARNING, "Error creating Kubernetes pod", e);
      return kj::READY_NOW;
    });
  } else {
    KJ_LOG(WARNING, "Cannot start container - no Kubernetes access");
    return kj::READY_NOW;
  }
}

kj::Promise<void> KubernetesContainerService::monitor(MonitorContext context) {
  auto paf = kj::newPromiseAndFulfiller<void>();
  monitorFulfiller = kj::mv(paf.fulfiller);
  return kj::mv(paf.promise);
}

kj::Promise<void> KubernetesContainerService::destroy(DestroyContext context) {
  if (running) {
    KJ_IF_SOME(client, getHttpClient()) {
      auto headers = createKubernetesHeaders();
      auto url = kj::str("https://", apiServer, "/api/v1/namespaces/", namespace_, "/pods/", getPodName());

      auto request = client.request(kj::HttpMethod::DELETE, url, *headers);

      return request.response.then(
          [this](kj::HttpClient::Response&& response) -> kj::Promise<void> {
        if (response.statusCode == 200 || response.statusCode == 202) {
          running = false;
          KJ_IF_SOME(f, monitorFulfiller) {
            f->fulfill();
            monitorFulfiller = kj::none;
          }
        } else {
          KJ_LOG(WARNING, "Failed to delete Kubernetes pod", response.statusCode);
        }
        return kj::READY_NOW;
      }).catch_([this](kj::Exception&& e) -> kj::Promise<void> {
        KJ_LOG(WARNING, "Error deleting Kubernetes pod", e);
        running = false;
        KJ_IF_SOME(f, monitorFulfiller) {
          f->fulfill();
          monitorFulfiller = kj::none;
        }
        return kj::READY_NOW;
      });
    } else {
      running = false;
      KJ_IF_SOME(f, monitorFulfiller) {
        f->fulfill();
        monitorFulfiller = kj::none;
      }
    }
  }
  return kj::READY_NOW;
}

kj::Promise<void> KubernetesContainerService::signal(SignalContext context) {
  KJ_REQUIRE(running, "Container is not running");

  // Kubernetes doesn't directly support sending signals
  // For SIGTERM/SIGKILL we can use pod deletion with grace period
  uint32_t signo = context.getParams().getSigno();

  if (signo == 15) { // SIGTERM
    KJ_IF_SOME(client, getHttpClient()) {
      auto headers = createKubernetesHeaders();
      auto url = kj::str("https://", apiServer, "/api/v1/namespaces/", namespace_, "/pods/", getPodName());

      // For a DELETE with grace period, we need to send a JSON body
      auto body = kj::str("{\"gracePeriodSeconds\":30,\"propagationPolicy\":\"Foreground\"}");
      auto request = client.request(kj::HttpMethod::DELETE, url, *headers, body.size());
      auto writePromise = request.body->write(body.asBytes());

      return request.response.then(
          [](kj::HttpClient::Response&& response) -> kj::Promise<void> {
        switch (response.statusCode) {
          case 200: // OK
          case 202: // Accepted - this is common for deletions
            return kj::READY_NOW;
          default:
            return response.body->readAllBytes().then(
                [&response](kj::Array<byte> body) -> kj::Promise<void> {
              auto errorBody = kj::StringPtr((const char*)body.begin(), body.size());
              KJ_LOG(WARNING, "Failed to send SIGTERM to container",
                    response.statusCode, errorBody);
              return kj::READY_NOW;
            });
        }
      }).catch_([](kj::Exception&& e) -> kj::Promise<void> {
        KJ_LOG(WARNING, "Error sending SIGTERM to container", e);
        return kj::READY_NOW;
      });
    }
  } else if (signo == 9) { // SIGKILL
    KJ_IF_SOME(client, getHttpClient()) {
      auto headers = createKubernetesHeaders();
      auto url = kj::str("https://", apiServer, "/api/v1/namespaces/", namespace_, "/pods/", getPodName());

      // For SIGKILL, we want immediate termination
      auto body = kj::str("{\"gracePeriodSeconds\":0,\"propagationPolicy\":\"Foreground\"}");
      auto request = client.request(kj::HttpMethod::DELETE, url, *headers, body.size());
      auto writePromise = request.body->write(body.asBytes());

      return request.response.then(
          [](kj::HttpClient::Response&& response) -> kj::Promise<void> {
        switch (response.statusCode) {
          case 200: // OK
          case 202: // Accepted
            return kj::READY_NOW;
          default:
            return response.body->readAllBytes().then(
                [&response](kj::Array<byte> body) -> kj::Promise<void> {
              auto errorBody = kj::StringPtr((const char*)body.begin(), body.size());
              KJ_LOG(WARNING, "Failed to send SIGKILL to container",
                    response.statusCode, errorBody);
              return kj::READY_NOW;
            });
        }
      }).catch_([](kj::Exception&& e) -> kj::Promise<void> {
        KJ_LOG(WARNING, "Error sending SIGKILL to container", e);
        return kj::READY_NOW;
      });
    }
  } else {
    // For other signals, we'd need to exec into the container
    // Implement exec API call for other signals
    KJ_LOG(WARNING, "Signal not directly supported in Kubernetes implementation", signo);

    // For signals other than TERM/KILL, we can try using the exec API
    // but this would need to be implemented properly
    return kj::READY_NOW;
  }

  return kj::READY_NOW;
}

kj::Promise<void> KubernetesContainerService::getTcpPort(GetTcpPortContext context) {
  KJ_REQUIRE(running, "Container is not running");
  uint16_t port = context.getParams().getPort();

  // Set up port forwarding to the Kubernetes pod
  context.getResults().setPort(
      kj::heap<KubernetesPortImpl>(getPodName(), namespace_, port, getHttpClient(), apiServer)
  );
  return kj::READY_NOW;
}

kj::Promise<void> KubernetesContainerService::listenTcp(ListenTcpContext context) {
  // This would require setting up a special proxy in Kubernetes
  // that can intercept outbound connections
  KJ_UNIMPLEMENTED("TCP interception not implemented for Kubernetes");
}

kj::Maybe<kj::HttpClient&> KubernetesContainerService::getHttpClient() {
  // TODO: If httpClient is not set, create one on demand
  return httpClient;
}

kj::String KubernetesContainerService::getPodName() {
  // Generate consistent pod name from actorId
  // Must conform to Kubernetes naming rules (lowercase alphanumeric, max 63 chars)

  // Replace invalid characters with dashes
  auto sanitized = kj::heapString(actorId);
  for (auto& c : sanitized) {
    if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) {
      c = '-';
    }
  }

  // Ensure it starts with alpha
  if (sanitized.size() > 0 && !(sanitized[0] >= 'a' && sanitized[0] <= 'z')) {
    sanitized = kj::str("do-", sanitized);
  }

  // Truncate if too long, max 63 chars for Kubernetes pod names
  if (sanitized.size() > 63) {
    sanitized = kj::heapString(sanitized.slice(0, 63));
  }

  return sanitized;
}

kj::Own<kj::HttpHeaders> KubernetesContainerService::createKubernetesHeaders() {
  static const kj::HttpHeaderTable headerTable;
  auto headers = kj::heap<kj::HttpHeaders>(headerTable);
  headers->set(kj::HttpHeaderId::CONTENT_TYPE, "application/json");

  // Add authentication header if token is available
  if (serviceAccountToken.size() > 0) {
    headers->add("Authorization", kj::str("Bearer ", serviceAccountToken));
  }

  return headers;
}

kj::String KubernetesContainerService::buildPodSpec(kj::ArrayPtr<kj::String> entrypoint,
                           kj::ArrayPtr<kj::String> envVars,
                           bool enableInternet) {
  // Build Kubernetes pod specification JSON
  kj::Vector<kj::String> envJsonParts;
  for (auto& env : envVars) {
    // Manual splitting since kj::String doesn't have split()
    for (size_t i = 0; i < env.size(); i++) {
      if (env[i] == '=') {
        auto name = kj::heapString(env.slice(0, i));
        auto value = kj::heapString(env.slice(i + 1));
        envJsonParts.add(kj::str(
            "{\"name\":\"", name, "\",\"value\":\"", value, "\"}"));
        break;
      }
    }
  }

  kj::Vector<kj::String> cmdParts;
  for (auto& cmd : entrypoint) {
    // Manually escape quotes since kj::String doesn't have replaceAll()
    kj::String escaped = kj::heapString(cmd);
    kj::Vector<char> result;
    for (char c : escaped) {
      if (c == '"') {
        result.add('\\');
      }
      result.add(c);
    }
    result.add('\0');
    kj::String escapedStr = kj::String(result.releaseAsArray());
    cmdParts.add(kj::str("\"", escapedStr, "\""));
  }

  return kj::str(
      "{"
      "\"apiVersion\":\"v1\","
      "\"kind\":\"Pod\","
      "\"metadata\":{"
      "  \"name\":\"", getPodName(), "\","
      "  \"labels\":{"
      "    \"app\":\"workerd-container\","
      "    \"actorId\":\"", actorId, "\""
      "  }"
      "},"
      "\"spec\":{"
      "  \"containers\":[{"
      "    \"name\":\"main\","
      "    \"image\":\"", image, "\","
      "    \"command\":[", kj::strArray(cmdParts, ","), "],"
      "    \"env\":[", kj::strArray(envJsonParts, ","), "],"
      "    \"resources\":{"
      "      \"limits\":{"
      "        \"memory\":\"", memoryLimitMb, "Mi\","
      "        \"cpu\":\"", cpuLimitMillicores, "m\""
      "      }"
      "    },"
      "    \"securityContext\":{"
      "      \"runAsNonRoot\":", runAsNonRoot ? "true" : "false",
      "      \"allowPrivilegeEscalation\":false"
      "    }"
      "  }],"
      "  \"restartPolicy\":\"Never\""
      "}"
      "}");
}

// KubernetesPortImpl implementation
KubernetesContainerService::KubernetesPortImpl::KubernetesPortImpl(
    kj::StringPtr podName, kj::StringPtr namespace_,
    uint16_t port, kj::Maybe<kj::HttpClient&> httpClient, kj::StringPtr apiServer)
    : podName(kj::heapString(podName)),
      namespace_(kj::heapString(namespace_)),
      port(port),
      httpClient(httpClient),
      apiServer(kj::heapString(apiServer)) {}

kj::Promise<void> KubernetesContainerService::KubernetesPortImpl::connect(ConnectContext context) {
  // Set up port forwarding to the pod via Kubernetes API
  KJ_IF_SOME(client, httpClient) {
    auto downStream = context.getParams().getDown();
    auto results = context.getResults();

    // Create a URL for port forwarding to the pod
    auto url = kj::str("https://", apiServer, "/api/v1/namespaces/", namespace_,
                      "/pods/", podName, "/portforward?ports=", port);

    // Set up headers for port forwarding request
    static const kj::HttpHeaderTable headerTable;
    auto headers = kj::heap<kj::HttpHeaders>(headerTable);
    headers->set(kj::HttpHeaderId::CONTENT_TYPE, "application/octet-stream");

    // Add authentication header using service account token
    static kj::String tokenStr = kj::str("service-account-token");
    headers->add("Authorization", kj::str("Bearer ", tokenStr));

    // Kubernetes port-forwarding protocol uses SPDY or websockets
    headers->add("Upgrade", "SPDY/3.1");
    headers->add("Connection", "Upgrade");

    // Create the request
    auto request = client.request(kj::HttpMethod::POST, url, *headers);

    return request.response.then(
        [this, downStream = kj::mv(downStream), &results]
        (kj::HttpClient::Response&& response) mutable -> kj::Promise<void> {
      if (response.statusCode == 101 || response.statusCode == 200) {
        // Create a ByteStream adapter to handle the port forwarding protocol
        auto byteStreamAdapter = kj::refcounted<ByteStreamAdapter>();

        // Check if we can get a bidirectional stream
        auto* ioStream = dynamic_cast<kj::AsyncIoStream*>(response.body.get());
        if (ioStream != nullptr) {
          // Perfect! We have a bidirectional stream
          byteStreamAdapter->setConnection(ioStream);
          byteStreamAdapter->setOutputStream(ioStream);
          KJ_LOG(INFO, "Successfully set up bidirectional connection for port forwarding");
        } else {
          // Try to get output stream separately if bidirectional not available
          byteStreamAdapter->setConnection(response.body.get());

          auto* outputStream = dynamic_cast<kj::AsyncOutputStream*>(response.body.get());
          if (outputStream != nullptr) {
            byteStreamAdapter->setOutputStream(outputStream);
            KJ_LOG(INFO, "Set up streams for port forwarding");
          } else {
            KJ_LOG(WARNING, "No output stream available, write operations will fail");
          }
        }

        // Set up the upstream connection (pod to client)
        auto adapterCopy = kj::addRef(*byteStreamAdapter);
        results.setUp(kj::mv(byteStreamAdapter));

        // Forward data from the client to the pod
        // Connect the downstream to our port forwarding connection
        auto pumpDownPromise = pumpDataFromClient(downStream, kj::mv(adapterCopy));

        KJ_LOG(INFO, "Successfully established port forwarding to pod", podName, port);
        return pumpDownPromise;
      } else {
        // Failed to establish port forwarding
        return response.body->readAllBytes().then(
            [this, &results, &response](kj::Array<byte> body) -> kj::Promise<void> {
          auto errorBody = kj::StringPtr((const char*)body.begin(), body.size());
          KJ_LOG(WARNING, "Failed to establish port forwarding to pod",
                podName, port, response.statusCode, errorBody);

          // Signal that connection is closed
          results.setUp(kj::heap<EmptyByteStream>());
          return kj::READY_NOW;
        });
      }
    }).catch_([this, &results](kj::Exception&& e) -> kj::Promise<void> {
      KJ_LOG(WARNING, "Error establishing port forwarding to pod", podName, port, e);
      results.setUp(kj::heap<EmptyByteStream>());
      return kj::READY_NOW;
    });
  } else {
    // No HTTP client available
    KJ_LOG(WARNING, "Cannot establish port forwarding - no Kubernetes client available");
    context.getResults().setUp(kj::heap<EmptyByteStream>());
    return kj::READY_NOW;
  }
}

// ByteStreamAdapter implementations
KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::ByteStreamAdapter()
    : active(true), connection(nullptr) {}

void KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::setConnection(kj::AsyncInputStream* inputStream) {
  inputConnection = inputStream;
  if (inputStream != nullptr) {
    startReading();
  }
}

void KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::setOutputStream(kj::AsyncOutputStream* outputStream) {
  outputConnection = outputStream;
}

kj::Promise<void> KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::registerDataCallback(kj::Promise<void> awaitData) {
  auto paf = kj::newPromiseAndFulfiller<void>();
  streamDataFulfillers.add(kj::mv(paf.fulfiller));
  return awaitData.attach(kj::mv(paf.promise));
}

kj::Promise<kj::Maybe<kj::Array<byte>>> KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::readNextData() {
  auto paf = kj::newPromiseAndFulfiller<kj::Maybe<kj::Array<byte>>>();
  dataFulfillers.add(kj::mv(paf.fulfiller));
  return kj::mv(paf.promise);
}

KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::~ByteStreamAdapter() noexcept(false) {
  active = false;
  connection = nullptr;
  inputConnection = kj::none;
  outputConnection = kj::none;
}

kj::Promise<void> KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::write(WriteContext context) {
  if (!active) {
    return kj::READY_NOW;
  }

  try {
    auto bytes = context.getParams().getBytes();
    if (bytes.size() == 0) {
      KJ_LOG(WARNING, "Empty data received in write");
      return kj::READY_NOW;
    }

    // Format the data according to Kubernetes port forwarding protocol
    // Channel 0 (stdin) for outgoing data to the pod
    kj::Vector<byte> buffer(5 + bytes.size());
    buffer.add(0); // Channel 0 (stdin)

    // Add 4-byte big-endian size
    uint32_t size = bytes.size();
    buffer.add((size >> 24) & 0xFF);
    buffer.add((size >> 16) & 0xFF);
    buffer.add((size >> 8) & 0xFF);
    buffer.add(size & 0xFF);

    // Add the actual data
    buffer.addAll(bytes);
    auto array = buffer.releaseAsArray();

    KJ_LOG(INFO, "Forwarding data to container", size);

    KJ_IF_SOME(output, outputConnection) {
      return output->write(array.asBytes());
    } else {
      KJ_LOG(WARNING, "No output connection available for writing");
    }

    return kj::READY_NOW;
  } catch (kj::Exception& e) {
    KJ_LOG(WARNING, "Error in port forward write", e);
    active = false;
    return kj::READY_NOW;
  }
}

kj::Promise<void> KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::end(EndContext context) {
  active = false;
  connection = nullptr;
  inputConnection = kj::none;
  outputConnection = kj::none;
  return kj::READY_NOW;
}

void KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::startReading() {
  KJ_IF_SOME(input, inputConnection) {
    if (active) {
      auto buffer = kj::heapArray<byte>(5);
      readHeader(input, kj::mv(buffer))
          .attach(kj::addRef(*this))
          .detach([this](kj::Exception&& e) {
            active = false;
            KJ_LOG(WARNING, "Error in port forwarding read loop", e);
          });
    }
  }
}

kj::Promise<void> KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::readHeader(
    kj::AsyncInputStream* input, kj::Array<byte> headerBuffer) {
  if (!active) {
    return kj::READY_NOW;
  }

  return input->tryRead(headerBuffer.begin(), headerBuffer.size(), headerBuffer.size())
      .then([this, input, headerBuffer = kj::mv(headerBuffer)]
            (size_t bytesRead) mutable -> kj::Promise<void> {
    if (bytesRead == 0) {
      // Connection closed
      active = false;
      KJ_LOG(INFO, "Port forwarding connection closed");
      return kj::READY_NOW;
    }

    if (bytesRead < 5) {
      // Read the remaining header bytes
      return input->tryRead(headerBuffer.begin() + bytesRead,
                          headerBuffer.size() - bytesRead,
                          headerBuffer.size() - bytesRead)
          .then([this, input, headerBuffer = kj::mv(headerBuffer), bytesRead]
                (size_t moreBytes) mutable -> kj::Promise<void> {
        if (bytesRead + moreBytes < 5) {
          KJ_LOG(WARNING, "Incomplete port forwarding header received",
                bytesRead + moreBytes);
          active = false;
          return kj::READY_NOW;
        }

        return processHeader(input, kj::mv(headerBuffer));
      });
    }

    return processHeader(input, kj::mv(headerBuffer));
  });
}

kj::Promise<void> KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::processHeader(
    kj::AsyncInputStream* input, kj::Array<byte> headerBuffer) {
  if (!active) {
    return kj::READY_NOW;
  }

  // Parse header: [channel byte][4-byte big-endian size]
  byte channel = headerBuffer[0];
  uint32_t size = (static_cast<uint32_t>(headerBuffer[1]) << 24) |
                  (static_cast<uint32_t>(headerBuffer[2]) << 16) |
                  (static_cast<uint32_t>(headerBuffer[3]) << 8) |
                  static_cast<uint32_t>(headerBuffer[4]);

  KJ_LOG(INFO, "Received port forwarding header", channel, size);

  if (size > 1024 * 1024) {
    // Sanity check - reject unreasonably large packets
    KJ_LOG(WARNING, "Rejecting excessively large port forward packet", size);
    active = false;
    return kj::READY_NOW;
  }

  auto payloadBuffer = kj::heapArray<byte>(size);
  return readPayload(input, kj::mv(payloadBuffer), channel, size);
}

kj::Promise<void> KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::readPayload(
    kj::AsyncInputStream* input, kj::Array<byte> payloadBuffer, byte channel, uint32_t size) {
  if (!active || size == 0) {
    if (active) {
      auto nextHeaderBuffer = kj::heapArray<byte>(5);
      return readHeader(input, kj::mv(nextHeaderBuffer));
    }
    return kj::READY_NOW;
  }

  return input->read(payloadBuffer.begin(), payloadBuffer.size(), size)
      .then([this, input, payloadBuffer = kj::mv(payloadBuffer), channel]
            (size_t bytesRead) mutable -> kj::Promise<void> {

    if (bytesRead == 0) {
      // Connection closed
      active = false;
      return kj::READY_NOW;
    }

    // Process the payload data
    processPayload(channel, kj::mv(payloadBuffer));

    // Continue reading next message
    if (active) {
      auto nextHeaderBuffer = kj::heapArray<byte>(5);
      return readHeader(input, kj::mv(nextHeaderBuffer));
    }

    return kj::READY_NOW;
  });
}

void KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::processPayload(
    byte channel, kj::Array<byte> payload) {
  if (!active) return;

  switch (channel) {
    case 1: { // stdout
      KJ_LOG(INFO, "Received stdout data from port forwarding", payload.size());
      // Forward this data to the client
      forwardStreamData(kj::mv(payload), true);
      break;
    }
    case 2: { // stderr
      KJ_LOG(WARNING, "Received stderr data from port forwarding", payload.size());
      // Forward this data to the client as error stream
      forwardStreamData(kj::mv(payload), false);
      break;
    }
    case 3: { // error
      KJ_LOG(ERROR, "Error from port forwarding",
            kj::StringPtr((const char*)payload.begin(), payload.size()));
      break;
    }
    default:
      KJ_LOG(WARNING, "Received data on unknown channel", channel);
      break;
  }
}

void KubernetesContainerService::KubernetesPortImpl::ByteStreamAdapter::forwardStreamData(
    kj::Array<byte> data, bool isStdout) {
  if (!active || data.size() == 0) {
    return;
  }

  KJ_LOG(INFO, "Forwarding data to client", isStdout ? "stdout" : "stderr", data.size());

  // Create a copy of the data that can be shared across multiple promises
  auto sharedData = kj::heap<kj::Array<byte>>(kj::mv(data));

  // Notify anyone waiting for data
  for (size_t i = 0; i < streamDataFulfillers.size(); i++) {
    auto& fulfiller = streamDataFulfillers[i];
    if (fulfiller->isWaiting()) {
      fulfiller->fulfill();
    }
  }

  // Provide actual data to any data callbacks
  for (size_t i = 0; i < dataFulfillers.size(); i++) {
    auto& fulfiller = dataFulfillers[i];
    if (fulfiller->isWaiting()) {
      // Create a copy of the data for each fulfiller
      kj::Array<byte> dataCopy = kj::heapArray<byte>(sharedData->size());
      memcpy(dataCopy.begin(), sharedData->begin(), sharedData->size());
      fulfiller->fulfill(kj::Maybe<kj::Array<byte>>(kj::mv(dataCopy)));
    }
  }

  // Clean up fulfilled streamDataFulfillers
  for (size_t i = streamDataFulfillers.size(); i-- > 0;) {
    if (!streamDataFulfillers[i]->isWaiting()) {
      std::swap(streamDataFulfillers[i], streamDataFulfillers.back());
      streamDataFulfillers.removeLast();
    }
  }

  // Clean up fulfilled dataFulfillers
  for (size_t i = dataFulfillers.size(); i-- > 0;) {
    if (!dataFulfillers[i]->isWaiting()) {
      std::swap(dataFulfillers[i], dataFulfillers.back());
      dataFulfillers.removeLast();
    }
  }
}

kj::Promise<void> KubernetesContainerService::KubernetesPortImpl::pumpDataFromClient(
    capnp::ByteStream::Client downstream, kj::Own<ByteStreamAdapter> adapter) {
  class PumpData {
  public:
    PumpData(capnp::ByteStream::Client downstream, kj::Own<ByteStreamAdapter> adapter)
        : downstream(kj::mv(downstream)), adapter(kj::mv(adapter)), active(true) {}

    ~PumpData() {
      active = false;
    }

    kj::Promise<void> start() {
      return loop();
    }

  private:
    capnp::ByteStream::Client downstream;
    kj::Own<ByteStreamAdapter> adapter;
    bool active;

    kj::Promise<void> loop() {
      if (!active) {
        return kj::READY_NOW;
      }

      // Read data from the client and forward it to the pod
      return adapter->readNextData().then(
          [this](kj::Maybe<kj::Array<byte>> maybeData) -> kj::Promise<void> {
        KJ_IF_SOME(data, maybeData) {
          if (data.size() > 0 && active) {
            auto req = downstream.writeRequest();
            req.setBytes(data);
            return req.send().then([this]() -> kj::Promise<void> {
              return loop();
            });
          }
        }

        // End of stream or no data
        if (active) {
          return loop();
        }
        return kj::READY_NOW;
      }).catch_([this](kj::Exception&& e) -> kj::Promise<void> {
        if (active) {
          KJ_LOG(WARNING, "Error reading data from pod", e);
          active = false;
        }
        return kj::READY_NOW;
      });
    }
  };

  auto pump = kj::heap<PumpData>(kj::mv(downstream), kj::mv(adapter));
  auto promise = pump->start();
  return promise.attach(kj::mv(pump));
}

kj::Promise<void> KubernetesContainerService::KubernetesPortImpl::EmptyByteStream::write(WriteContext context) {
  // Just immediately return without writing anything
  return kj::READY_NOW;
}

kj::Promise<void> KubernetesContainerService::KubernetesPortImpl::EmptyByteStream::end(EndContext context) {
  return kj::READY_NOW;
}

} // namespace workerd::server
