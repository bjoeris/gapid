/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <thread>
#include "gapii/cc/vulkan_types.h"

#ifndef GAPII_VULKAN_EXTERNAL_SYNCHRONIZATION_H_
#define GAPII_VULKAN_EXTERNAL_SYNCHRONIZATION_H_

namespace gapii {

enum class ExternalFenceStatus : uint32_t {
  Unsignaled = VkResult::VK_TIMEOUT,
  Signaled = VkResult::VK_SUCCESS,
};

enum class SignalSource {
  Local = 0,
  External = 1,
};

struct ExternalFenceState {
  // `appFence` is the original fence created by the application
  // `appFence` is owned by `appDevice`.
  VkFence appFence = 0;

  // `externAppFence` has the same payload as `appFence`, but is owned by `externDevice`
  VkFence externAppFence = 0;

  // `externFence` shares the fence payload with the external device/process
  // `externFence` is owned by `externDevice`
  VkFence externFence = 0;

  ExternalFenceStatus status = ExternalFenceStatus::Unsignaled;

  bool isPermanent = false;
};

class VulkanSpy;

class ExternalSyncState {
public:
  ExternalSyncState(const ExternalSyncState&) = delete;
  ExternalSyncState(ExternalSyncState&& other);
  inline ExternalSyncState(VulkanSpy *spy, VkDevice device) : spy(spy), appDevice(device) {}
  ~ExternalSyncState();
  uint32_t Initialize();
  // Returns a new fence that should replace the original fence in the queue submission.
  // VkFence submitExternalFence(VkFence fence);
  //void ExportFence(VkFence fence);
  void updateFences(CallObserver *observer);
  uint32_t waitForFences(CallObserver *observer, uint32_t fenceCount, const VkFence *pFences, VkBool32 waitAll, uint64_t timeout);

  uint32_t createExportFence(VkFence appFence, const VkExportFenceCreateInfo &info);
  uint32_t getFenceFd(const VkFenceGetFdInfoKHR &info, int* fd);
  uint32_t importFenceFd(const VkImportFenceFdInfoKHR &info);
  uint32_t resetFences(uint32_t fenceCount, const VkFence* pFences);

protected:
  VulkanSpy *spy = nullptr;
  VkDevice appDevice = 0;
  VkDevice externDevice = 0;
  VkQueue fenceQueue = 0;
  std::map<VkFence, ExternalFenceState> fences;
  // std::thread fenceThread;
  // std::atomic<bool> fenceThreadShouldQuit;
  // static const uint64_t kFenceThreadTimeout = 10000000; // 10ms

  void sendFenceSignaled(VkFence fence, SignalSource source, CallObserver *observer);
  void sendFenceReset(VkFence fence, CallObserver *observer);
  void setFenceSignaled(VkFence fence);
  void destroyFenceState(ExternalFenceState &fenceState);
  // void fenceThreadEntry();
};


}  // namespace gapii

#endif  // GAPII_VULKAN_EXTERNAL_SYNCHRONIZATION_H_
