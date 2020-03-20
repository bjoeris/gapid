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

#include "core/cc/get_vulkan_proc_address.h"

#include "gapii/cc/vulkan_spy.h"
#include "gapii/cc/vulkan_layer_extras.h"
#include "gapii/cc/vulkan_external_synchronization.h"
#include "gapis/api/vulkan/vulkan_pb/extras.pb.h"

namespace gapii {

uint32_t VulkanSpy::SpyOverride_vkWaitForFences(
    CallObserver*                               observer, 
    VkDevice                                    device,
    uint32_t                                    fenceCount,
    const VkFence*                              pFences,
    VkBool32                                    waitAll,
    uint64_t                                    timeout) {
  if(external_sync().HasExternDevice(device)) {
    return external_sync().GetOrCreateExternDevice(device).waitForFences(observer, fenceCount, pFences, waitAll, timeout);
  } else {
    return mImports.mVkDeviceFunctions[device].vkWaitForFences(device, fenceCount, pFences, waitAll, timeout);
  }
}

uint32_t VulkanSpy::SpyOverride_vkGetFenceFdKHR(
    CallObserver*                               observer, 
    VkDevice                                    device,
    const VkFenceGetFdInfoKHR*                  pGetFdInfo,
    int*                                        pFd) {
  if(external_sync().HasExternDevice(device)) {
    return external_sync().GetOrCreateExternDevice(device).getFenceFd(*pGetFdInfo, pFd);
  } else {
    GAPID_ERROR("VkGetFenceFd called for device with no ExternalSyncState");
    return VkResult::VK_ERROR_VALIDATION_FAILED_EXT;
  }
}

uint32_t VulkanSpy::SpyOverride_vkResetFences(
    CallObserver*                               observer,
    VkDevice                                    device,
    uint32_t                                    fenceCount,
    const VkFence*                              pFences) {
  if(external_sync().HasExternDevice(device)) {
    return external_sync().GetOrCreateExternDevice(device).resetFences(fenceCount, pFences);
  } else {
    return mImports.mVkDeviceFunctions[device].vkResetFences(device, fenceCount, pFences);
  }
}

uint32_t VulkanSpy::SpyOverride_vkImportFenceFdKHR(
    CallObserver*                               observer,
    VkDevice                                    device,
    const VkImportFenceFdInfoKHR*               pImportFenceFdInfo) {
  return external_sync().GetOrCreateExternDevice(device).importFenceFd(*pImportFenceFdInfo);
}
uint32_t VulkanSpy::SpyOverride_vkCreateFence(
    CallObserver*                               observer,
    VkDevice                                    device,
    const VkFenceCreateInfo*                    pCreateInfo,
    AllocationCallbacks                         pAllocator,
    VkFence*                                    pFence) {
  auto fn = mImports.mVkDeviceFunctions[device];
  
  uint32_t res = fn.vkCreateFence(device, pCreateInfo, pAllocator, pFence);
  if(res != VkResult::VK_SUCCESS)
    return res;

  const VulkanStructHeader *pNext = reinterpret_cast<const VulkanStructHeader*>(pCreateInfo->mpNext);
  while(pNext != nullptr) {
    if(pNext->mSType == VkStructureType::VK_STRUCTURE_TYPE_EXPORT_FENCE_CREATE_INFO) {
      return external_sync().GetOrCreateExternDevice(device).createExportFence(*pFence, *reinterpret_cast<const VkExportFenceCreateInfo*>(pNext));
    }
    pNext = reinterpret_cast<const VulkanStructHeader*>(pNext->mPNext);
  }
  
  return VkResult::VK_SUCCESS;
}

ExternalSyncState& VulkanSpy::external_sync() {
  if (mExternalSync == nullptr) {
    mExternalSync = new ExternalSyncState(this);
  }
  return *mExternalSync;
}

uint32_t ExternalSyncDeviceState::Initialize() {
  if (mExternDevice != 0)
    return VkResult::VK_SUCCESS;

  uint32_t physical_device_count = 1;
  VkPhysicalDevice physical_device = 0;
  uint32_t res = mParent->functions().vkEnumeratePhysicalDevices(mParent->instance(), &physical_device_count, &physical_device);
  if(res != VkResult::VK_SUCCESS && res != VkResult::VK_INCOMPLETE) {
    GAPID_ERROR("Failed to enemurate physical devices for ExternalSyncDeviceState");
    return res;
  }

  VkDeviceCreateInfo info(mSpy->arena());
  info.msType = VkStructureType::VK_STRUCTURE_TYPE_DEVICE_CREATE_INFO;
  info.mqueueCreateInfoCount = 1;
  VkDeviceQueueCreateInfo queueInfo(mSpy->arena());
  queueInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_DEVICE_QUEUE_CREATE_INFO;
  queueInfo.mqueueCount = 1;
  float priority = 1.0f;
  queueInfo.mpQueuePriorities = &priority;
  info.mpQueueCreateInfos = &queueInfo;
  std::vector<const char *> extensions{
    "VK_KHR_get_physical_device_properties2",
    "VK_KHR_external_fence_capabilities",
    "VK_KHR_external_fence",
    "VK_KHR_external_fence_fd"
  };
  info.menabledExtensionCount = (uint32_t)extensions.size();
  info.mppEnabledExtensionNames = extensions.data();
  res = mParent->functions().vkCreateDevice(physical_device, &info, nullptr, &mExternDevice);
  if(res != VkResult::VK_SUCCESS){
    GAPID_ERROR("Failed to create logical device for ExternalSyncDeviceState");
    return res;
  }
  auto get_device_proc_addr = reinterpret_cast<VulkanImports::PFNVKGETDEVICEPROCADDR>(mParent->functions().vkGetInstanceProcAddr(mParent->instance(), "vkGetDeviceProcAddr"));
  InitializeDeviceFunctions(mExternDevice, get_device_proc_addr, &mFunctions);
    
  mFunctions.vkGetDeviceQueue(mExternDevice, 0, 0, &mFenceQueue);
  return VkResult::VK_SUCCESS;
}


uint32_t ExternalSyncState::Initialize() {
  if(mInstance != 0)
    return VkResult::VK_SUCCESS;
  auto gpa = reinterpret_cast<VulkanImports::PFNVKGETINSTANCEPROCADDR>(core::GetVulkanInstanceProcAddress(0, "vkGetInstanceProcAddr"));
  auto pfn_vkCreateInstance = reinterpret_cast<VulkanImports::PFNVKCREATEINSTANCE>(gpa(0, "vkCreateInstance"));
  VkInstanceCreateInfo create_info(mSpy->arena());
  create_info.msType = VkStructureType::VK_STRUCTURE_TYPE_INSTANCE_CREATE_INFO;
  std::vector<const char *> extensions = {
    "VK_KHR_external_fence_capabilities",
  };
  create_info.menabledExtensionCount = extensions.size();
  create_info.mppEnabledExtensionNames = extensions.data();
  return pfn_vkCreateInstance(&create_info, nullptr, &mInstance);
}

ExternalSyncDeviceState::ExternalSyncDeviceState(ExternalSyncDeviceState&& other) {
  mSpy = other.mSpy;
  other.mSpy = nullptr;
  mAppDevice = other.mAppDevice;
  other.mAppDevice = 0;
  mExternDevice = other.mExternDevice;
  other.mExternDevice = 0;
  mFenceQueue = other.mFenceQueue;
  other.mFenceQueue = 0;
  fences = std::move(other.fences);
}

ExternalSyncDeviceState::~ExternalSyncDeviceState() {
  for(auto &e : fences) {
    destroyFenceState(e.second);
  }
  fences.clear();
  if (mExternDevice != 0) {
    functions().vkDestroyDevice(mExternDevice, nullptr);
    mExternDevice = 0;
    mFenceQueue = 0;
  }
  mAppDevice = 0;
  mSpy = nullptr;
}

void ExternalSyncDeviceState::destroyFenceState(ExternalFenceState& fenceState) {
  functions().vkDestroyFence(mExternDevice, fenceState.externAppFence, nullptr);
  fenceState.externAppFence = 0;
  functions().vkDestroyFence(mExternDevice, fenceState.externFence, nullptr);
  fenceState.externFence = 0;
}

void ExternalSyncDeviceState::setFenceSignaled(VkFence fence) {
  VkSubmitInfo submitInfo(mSpy->arena());
  submitInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_SUBMIT_INFO;
  functions().vkQueueSubmit(mFenceQueue, 1, &submitInfo, fence);
  while(functions().vkGetFenceStatus(mExternDevice, fence) != VkResult::VK_SUCCESS) {
    // spin
  }
}

uint32_t ExternalSyncDeviceState::waitForFences(CallObserver *observer, uint32_t fenceCount, const VkFence *pFences, VkBool32 waitAll, uint64_t timeout) {
  std::vector<VkFence> externFences;
  for(uint32_t i=0; i<fenceCount; ++i) {
    auto it = fences.find(pFences[i]);
    if(it != fences.end()) {
      externFences.push_back(it->second.externFence);
    }
  }
  
  mSpy->unlock();
  if(externFences.empty()) {
    uint32_t res = functions().vkWaitForFences(mAppDevice, fenceCount, pFences, waitAll, timeout);
    mSpy->lock();
    return res;
  }
  std::atomic<bool> waitingExternFences(true);
  std::thread externFencesThread([&]() {
    functions().vkWaitForFences(mExternDevice, (uint32_t)externFences.size(), externFences.data(), waitAll, timeout);
    if(waitingExternFences.exchange(false)) {
      mSpy->lock();
      updateFences(observer);
      mSpy->unlock();
    }
  });
  uint32_t res = functions().vkWaitForFences(mAppDevice, fenceCount, pFences, waitAll, timeout);
  if(res == VkResult::VK_SUCCESS && waitingExternFences.exchange(false)) {
    mSpy->lock();
    return res;
  } else {
    externFencesThread.join();
    mSpy->lock();
    updateFences(observer);
    for(uint32_t i=0; i<fenceCount; ++i) {
      VkFence fence = pFences[i];
      ExternalFenceStatus status;
      auto it = fences.find(fence);
      if(it != fences.end()) {
        status = it->second.status;
      } else {
        status = (ExternalFenceStatus)functions().vkGetFenceStatus(mAppDevice, fence);
      }
      if(status == ExternalFenceStatus::Unsignaled && waitAll) {
        return VkResult::VK_TIMEOUT;
      } else if(status == ExternalFenceStatus::Signaled && !waitAll) {
        return VkResult::VK_SUCCESS;
      } else if (status != ExternalFenceStatus::Signaled && status != ExternalFenceStatus::Unsignaled) {
        return (uint32_t)status;
      }
    }
    if(waitAll)
      return VkResult::VK_SUCCESS;
    else
      return VkResult::VK_TIMEOUT;
  }
}

void ExternalSyncDeviceState::updateFences(CallObserver *observer) {
  
  for (auto &p: fences) {
    auto &e = p.second;
    ExternalFenceStatus externFenceStatus = (ExternalFenceStatus)functions().vkGetFenceStatus(mExternDevice, e.externFence);
    ExternalFenceStatus appFenceStatus = (ExternalFenceStatus)functions().vkGetFenceStatus(mExternDevice, e.externAppFence);
    switch(e.status) {
      case ExternalFenceStatus::Signaled:
      if (appFenceStatus == ExternalFenceStatus::Unsignaled && externFenceStatus == ExternalFenceStatus::Signaled) {
        // fence reset by application
        // nothing to send, since the vkResetFences call will be sent
        functions().vkResetFences(mExternDevice, 1, &e.externFence);
      } else if (appFenceStatus == ExternalFenceStatus::Signaled && externFenceStatus == ExternalFenceStatus::Unsignaled) {
        // fence reset externally
        // send a message recording this
        sendFenceReset(e.appFence, observer);
        functions().vkResetFences(mExternDevice, 1, &e.externAppFence);
      }
      if (externFenceStatus == ExternalFenceStatus::Unsignaled || appFenceStatus == ExternalFenceStatus::Unsignaled) {
        e.status = ExternalFenceStatus::Unsignaled;
      }
      break;
      case ExternalFenceStatus::Unsignaled:
      if (appFenceStatus == ExternalFenceStatus::Signaled && externFenceStatus == ExternalFenceStatus::Unsignaled) {
        setFenceSignaled(e.externFence);
        sendFenceSignaled(e.appFence, SignalSource::Local, observer);
        e.status = ExternalFenceStatus::Signaled;
      } else if (appFenceStatus == ExternalFenceStatus::Unsignaled && externFenceStatus == ExternalFenceStatus::Signaled) {
        setFenceSignaled(e.appFence);
        sendFenceSignaled(e.appFence, SignalSource::External, observer);
        e.status = ExternalFenceStatus::Signaled;
      } else if (appFenceStatus == ExternalFenceStatus::Signaled && externFenceStatus == ExternalFenceStatus::Signaled) {
        GAPID_ERROR("External fence signaled both internally and externally");
      }
      break;
    }
  }
}

void ExternalSyncDeviceState::sendFenceSignaled(VkFence fence, SignalSource source, CallObserver *observer) {
  auto extra = new vulkan_pb::FenceUpdate();
  extra->set_fence((uint64_t)fence);
  extra->set_source((vulkan_pb::SignalSource)source);
  extra->set_status(vulkan_pb::FenceStatus::SIGNALED);
  observer->encodeAndDelete(extra);
}

void ExternalSyncDeviceState::sendFenceReset(VkFence fence, CallObserver *observer) {
  auto extra = new vulkan_pb::FenceUpdate();
  extra->set_fence((uint64_t)fence);
  extra->set_source(vulkan_pb::SignalSource::EXTERNAL);
  extra->set_status(vulkan_pb::FenceStatus::UNSIGNALED);
  observer->encodeAndDelete(extra);
}

uint32_t ExternalSyncDeviceState::createExportFence(VkFence appFence, const VkExportFenceCreateInfo &exportInfo) {
  auto app_fn = mSpy->mImports.mVkDeviceFunctions[mAppDevice];
  ExternalFenceState fenceState;
  fenceState.isPermanent = true;
  fenceState.appFence = appFence;
  fenceState.status = (ExternalFenceStatus)app_fn.vkGetFenceStatus(mAppDevice, appFence);
  uint32_t res;
  {
    VkExportFenceCreateInfo exportInfoClone = exportInfo;
    exportInfoClone.mpNext = nullptr;
    VkFenceCreateInfo createInfo(mSpy->arena());
    createInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_FENCE_CREATE_INFO;
    createInfo.mpNext = &exportInfoClone;
    res = functions().vkCreateFence(mExternDevice, &createInfo, nullptr, &fenceState.externFence);
    if(res != VkResult::VK_SUCCESS)
      return res;
  }
  if((exportInfo.mhandleTypes&VkExternalFenceHandleTypeFlagBits::VK_EXTERNAL_FENCE_HANDLE_TYPE_OPAQUE_FD_BIT) != 0) {
    VkFenceGetFdInfoKHR fdInfo(mSpy->arena());
    fdInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_FENCE_GET_FD_INFO_KHR;
    fdInfo.mfence = appFence;
    fdInfo.mhandleType = VkExternalFenceHandleTypeFlagBits::VK_EXTERNAL_FENCE_HANDLE_TYPE_OPAQUE_FD_BIT;
    int fd;
    res = app_fn.vkGetFenceFdKHR(mAppDevice, &fdInfo, &fd);
    if(res != VkResult::VK_SUCCESS)
      return res;

    VkFenceCreateInfo createInfo(mSpy->arena());
    createInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_FENCE_CREATE_INFO;
    res = functions().vkCreateFence(mExternDevice, &createInfo, nullptr, &fenceState.externAppFence);
    if(res != VkResult::VK_SUCCESS)
      return res;

    VkImportFenceFdInfoKHR importInfo(mSpy->arena());
    importInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_IMPORT_FENCE_FD_INFO_KHR;
    importInfo.mfence = fenceState.externAppFence;
    importInfo.mhandleType = VkExternalFenceHandleTypeFlagBits::VK_EXTERNAL_FENCE_HANDLE_TYPE_OPAQUE_FD_BIT;
    importInfo.mfd = fd;
    res = functions().vkImportFenceFdKHR(mExternDevice, &importInfo);
    if (res != VkResult::VK_SUCCESS)
      return res;
  } else {
    GAPID_ERROR("Unknown external fence handle type (%d)", exportInfo.mhandleTypes);
    return VkResult::VK_ERROR_FEATURE_NOT_PRESENT;
  }
  fences.emplace(appFence, fenceState);
  return VkResult::VK_SUCCESS;
}

uint32_t ExternalSyncDeviceState::getFenceFd(const VkFenceGetFdInfoKHR &info, int *fd) {
  VkFenceGetFdInfoKHR externInfo(info);
  auto fenceIt = fences.find(info.mfence);
  if(fenceIt == fences.end()) {
    // This fence should have been created with `ExportFenceCreateInfo`, which should have created an entry in `fences`, so this should not happen under valid usage.
    GAPID_ERROR("getFenceFd called for unknown fence");
    return VkResult::VK_ERROR_VALIDATION_FAILED_EXT;
  }
  externInfo.mfence = fenceIt->second.externFence;
  
  return functions().vkGetFenceFdKHR(mExternDevice, &externInfo, fd);
}

uint32_t ExternalSyncDeviceState::importFenceFd(const VkImportFenceFdInfoKHR &importInfo) {
  if(importInfo.mhandleType != VkExternalFenceHandleTypeFlagBits::VK_EXTERNAL_FENCE_HANDLE_TYPE_OPAQUE_FD_BIT) {
    GAPID_ERROR("importFenceFd called with unknown fence handle type %d", importInfo.mhandleType);
    return VkResult::VK_ERROR_VALIDATION_FAILED_EXT;
  }
  uint32_t res;
  auto app_fn = mSpy->mImports.mVkDeviceFunctions[mAppDevice];
  ExternalFenceState& fenceState = fences[importInfo.mfence];
  if(fenceState.appFence == 0)
    fenceState.appFence = importInfo.mfence;
  //auto fenceIt = fences.find(importInfo.mfence);
  bool isPermanent = (importInfo.mflags & VkFenceImportFlagBits::VK_FENCE_IMPORT_TEMPORARY_BIT) == 0;
  if(isPermanent && !fenceState.isPermanent && fenceState.externAppFence != 0) {
    // Previously this fence had only been imported in temporary mode, but now importod as permanent.
    // Reset `externAppFence` so that we re-import it in permanent mode
    functions().vkDestroyFence(mExternDevice, fenceState.externAppFence, nullptr);
    fenceState.externAppFence = 0;
  }
  fenceState.isPermanent |= isPermanent;
  if(fenceState.externAppFence == 0) {
    VkExportFenceCreateInfo exportInfo(mSpy->arena());
    exportInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_EXPORT_FENCE_CREATE_INFO;
    exportInfo.mhandleTypes = importInfo.mhandleType;

    VkFenceCreateInfo createInfo(mSpy->arena());
    createInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_FENCE_CREATE_INFO;
    createInfo.mpNext = &exportInfo;

    res = functions().vkCreateFence(mExternDevice, &createInfo, nullptr, &fenceState.externAppFence);
    if(res != VkResult::VK_SUCCESS)
      return res;

    VkFenceGetFdInfoKHR fdInfo(mSpy->arena());
    fdInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_FENCE_GET_FD_INFO_KHR;
    fdInfo.mfence = fenceState.externAppFence;
    fdInfo.mhandleType = VkExternalFenceHandleTypeFlagBits::VK_EXTERNAL_FENCE_HANDLE_TYPE_OPAQUE_FD_BIT;
    int fd;
    res = functions().vkGetFenceFdKHR(mExternDevice, &fdInfo, &fd);
    if(res != VkResult::VK_SUCCESS)
      return res;

    VkImportFenceFdInfoKHR appImport(importInfo);
    appImport.mfd = fd;
    res = app_fn.vkImportFenceFdKHR(mAppDevice, &appImport);
    if(res != VkResult::VK_SUCCESS)
      return res;
  }
  if(fenceState.externFence == 0) {
    VkFenceCreateInfo createInfo(mSpy->arena());
    createInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_FENCE_CREATE_INFO;
    res = functions().vkCreateFence(mExternDevice, &createInfo, nullptr, &fenceState.externFence);
    if(res != VkResult::VK_SUCCESS)
      return res;
  }
  VkImportFenceFdInfoKHR extImport(importInfo);
  extImport.mfence = fenceState.externFence;
  res = functions().vkImportFenceFdKHR(mExternDevice, &extImport);
  if(res != VkResult::VK_SUCCESS)
    return res;
  return VkResult::VK_SUCCESS;
}

uint32_t ExternalSyncDeviceState::resetFences(uint32_t fenceCount, const VkFence* pFences) {
  std::vector<VkFence> appFences;
  std::vector<VkFence> externFences;
  std::vector<std::map<VkFence, ExternalFenceState>::iterator> destroy;
  for(uint32_t i=0; i<fenceCount; ++i) {
    VkFence f = pFences[i];
    auto fenceIt = fences.find(f);
    if(fenceIt == fences.end()) {
      appFences.push_back(f);
    } else if (!fenceIt->second.isPermanent) {
      appFences.push_back(fenceIt->second.appFence);
      destroy.push_back(fenceIt);
      fences.erase(fenceIt);
    } else {
      fenceIt->second.status = ExternalFenceStatus::Unsignaled;
      externFences.push_back(fenceIt->second.externAppFence);
      externFences.push_back(fenceIt->second.externFence);
    }
  }
  uint32_t res = VkResult::VK_SUCCESS;
  if(!appFences.empty()) {
    auto app_fn = mSpy->mImports.mVkDeviceFunctions[mAppDevice];
    uint32_t r = app_fn.vkResetFences(mAppDevice, (uint32_t)appFences.size(), appFences.data());
    if(r != VkResult::VK_SUCCESS)
      res = r;
  }
  if(!externFences.empty()) {
    uint32_t r = functions().vkResetFences(mExternDevice, (uint32_t)externFences.size(), externFences.data());
    if(r != VkResult::VK_SUCCESS)
      res = r;
  }
  for(auto fenceIt: destroy) {
    destroyFenceState(fenceIt->second);
  }
  return res;
}

ExternalSyncState::~ExternalSyncState() {
  mDevices.clear();
  if (mSpy && mInstance) {
    mSpy->mImports.mVkInstanceFunctions[mInstance].vkDestroyInstance(mInstance, nullptr);
  }
  mInstance = 0;
  mSpy = nullptr;
}

// void ExternalSyncState::fenceThreadEntry() {
//   std::vector<VkFence> waitFences;
//   while(!fenceThreadShouldQuit.load()) {
//     fences.clear();
//     for(const auto &e: fences) {
//       if (e.status == ExternalFenceStatus::Unsignaled) {
//         waitFences.push_back(e.internalFence);
//         waitFences.push_back(e.externalFence);
//       }
//     }
//     auto fn = spy->mImports.mVkDeviceFunctions[externalDevice];
//     fn.vkWaitForFences(externalDevice, (uint32_t)waitFences.size(), waitFences.data(), 0, kFenceThreadTimeout);
//   }
// }

}  // namespace gapii