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
class ExternalSyncState;

class ExternalSyncDeviceState {
public:
  ExternalSyncDeviceState(const ExternalSyncDeviceState&) = delete;
  ExternalSyncDeviceState(ExternalSyncDeviceState&& other);
  ExternalSyncDeviceState(ExternalSyncState *parent, VkDevice device);
  ~ExternalSyncDeviceState();
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

  inline const VulkanImports::VkDeviceFunctions &functions() const { return mFunctions; }

protected:
  VulkanImports::VkDeviceFunctions mFunctions;
  ExternalSyncState *mParent = nullptr;
  VulkanSpy *mSpy = nullptr;
  VkDevice mAppDevice = 0;
  VkDevice mExternDevice = 0;
  VkQueue mFenceQueue = 0;
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

class ExternalSyncState {
public:
  inline ExternalSyncState(VulkanSpy *spy) : mSpy(spy) {};
  ExternalSyncState(const ExternalSyncState&) = delete;
  inline ExternalSyncState(ExternalSyncState&& other) {
    mSpy = other.mSpy;
    other.mSpy = nullptr;
    mInstance = other.mInstance;
    other.mInstance = 0;
  }
  ~ExternalSyncState();

  inline bool HasExternDevice(VkDevice app_device) {
    return mDevices.find(app_device) != mDevices.end();
  }
  inline ExternalSyncDeviceState &GetOrCreateExternDevice(VkDevice app_device) {
    auto it = mDevices.find(app_device);
    if (it == mDevices.end()) {
      Initialize();
      it = mDevices.emplace(app_device, ExternalSyncDeviceState(this, app_device)).first;
      it->second.Initialize();
    }
    return it->second;
  }
  inline ExternalSyncDeviceState *FindExternDevice(VkDevice app_device) {
    auto it = mDevices.find(app_device);
    if (it == mDevices.end()) {
      return nullptr;
    }
    return &it->second;
  }
  inline VkInstance instance() const { return mInstance; }
  inline const VulkanImports::VkInstanceFunctions& functions() const { return mFunctions; }
  inline VulkanSpy *spy() const { return mSpy; }
protected:
  uint32_t Initialize();

  VulkanImports::VkInstanceFunctions mFunctions;
  std::unordered_map<VkDevice, ExternalSyncDeviceState> mDevices;
  VulkanSpy *mSpy = nullptr;
  VkInstance mInstance = 0;
};

inline ExternalSyncDeviceState::ExternalSyncDeviceState(ExternalSyncState *parent, VkDevice device) : mParent(parent), mSpy(mParent->spy()), mAppDevice(device) {}

}  // namespace gapii

#endif  // GAPII_VULKAN_EXTERNAL_SYNCHRONIZATION_H_
