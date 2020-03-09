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

#include "gapii/cc/vulkan_spy.h"
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
  auto it = mExternalSync.find(device);
  if(it == mExternalSync.end()) {
    return mImports.mVkDeviceFunctions[device].vkWaitForFences(device, fenceCount, pFences, waitAll, timeout);
  } else {
    return it->second.waitForFences(observer, fenceCount, pFences, waitAll, timeout);
  }
}

uint32_t VulkanSpy::SpyOverride_vkGetFenceFdKHR(
    CallObserver*                               observer, 
    VkDevice                                    device,
    const VkFenceGetFdInfoKHR*                  pGetFdInfo,
    int*                                        pFd) {
  auto it = mExternalSync.find(device);
  if(it == mExternalSync.end()) {
    GAPID_ERROR("VkGetFenceFd called for device with no ExternalSyncState");
  } else {
    return it->second.getFenceFd(*pGetFdInfo, pFd);
  }
}

uint32_t VulkanSpy::SpyOverride_vkResetFences(
    CallObserver*                               observer,
    VkDevice                                    device,
    uint32_t                                    fenceCount,
    const VkFence*                              pFences) {
  auto it = mExternalSync.find(device);
  if(it == mExternalSync.end()) {
    return mImports.mVkDeviceFunctions[device].vkResetFences(device, fenceCount, pFences);
  } else {
    return it->second.resetFences(fenceCount, pFences);
  }
}

uint32_t VulkanSpy::SpyOverride_vkImportFenceFdKHR(
    CallObserver*                               observer,
    VkDevice                                    device,
    const VkImportFenceFdInfoKHR*               pImportFenceFdInfo) {
  auto it = mExternalSync.find(device);
  if(it == mExternalSync.end()) {
    it = mExternalSync.emplace(device, ExternalSyncState(this, device)).first;
    uint32_t res = it->second.Initialize();
    if(res != VkResult::VK_SUCCESS)
      return res;
  }
  return it->second.importFenceFd(*pImportFenceFdInfo);
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

  const VkExportFenceCreateInfo *pExportInfo = nullptr;
  const VulkanStructHeader *pNext = reinterpret_cast<const VulkanStructHeader*>(pCreateInfo->mpNext);
  while(pNext != nullptr) {
    if(pNext->mSType == VkStructureType::VK_STRUCTURE_TYPE_EXPORT_FENCE_CREATE_INFO) {
      auto it = mExternalSync.find(device);
      if(it == mExternalSync.end()) {
        it = mExternalSync.emplace(device, ExternalSyncState(this, device)).first;
        uint32_t res = it->second.Initialize();
        if(res != VkResult::VK_SUCCESS)
          return res;
      }
      return it->second.createExportFence(*pFence, *reinterpret_cast<const VkExportFenceCreateInfo*>(pNext));
    }
  }
  
  return VkResult::VK_SUCCESS;
}

uint32_t ExternalSyncState::Initialize() {
  auto appDevObj = spy->mState.Devices[appDevice];
  VkPhysicalDevice physDev = appDevObj->mPhysicalDevice;
  auto physDevObj = spy->mState.PhysicalDevices[physDev];
  VkInstance instance = physDevObj->mInstance;
  auto instanceFn = spy->mImports.mVkInstanceFunctions[instance];
  if(externDevice == 0) {
    
    VkDeviceCreateInfo info(spy->arena());
    info.msType = VkStructureType::VK_STRUCTURE_TYPE_DEVICE_CREATE_INFO;
    info.mqueueCreateInfoCount = 1;
    VkDeviceQueueCreateInfo queueInfo(spy->arena);
    info.mpQueueCreateInfos = &queueInfo;
    std::vector<const char *> extensions{
      "VK_KHR_get_physical_device_properties2",
      "VK_KHR_external_fence_capabilities",
      "VK_KHR_external_fence",
      "VK_KHR_external_fence_fd"
    };
    info.menabledExtensionCount = (uint32_t)extensions.size();
    info.mppEnabledExtensionNames = extensions.data();
    uint32_t res = instanceFn.vkCreateDevice(physDev, &info, nullptr, &externDevice);
    if(res != VkResult::VK_SUCCESS)
      return res;
  }
  auto deviceFn =spy->mImports.mVkDeviceFunctions[externDevice];
  deviceFn.vkGetDeviceQueue(externDevice, 0, 0, &fenceQueue);
}

ExternalSyncState::~ExternalSyncState() {
  auto fn = spy->mImports.mVkDeviceFunctions[externDevice];
  for(auto &e : fences) {
    destroyFenceState(e.second);
  }
  fences.clear();
  fn.vkDestroyDevice(externDevice, nullptr);
  externDevice = 0;
}

void ExternalSyncState::destroyFenceState(ExternalFenceState& fenceState) {
  auto fn = spy->mImports.mVkDeviceFunctions[externDevice];
  fn.vkDestroyFence(externDevice, fenceState.externAppFence, nullptr);
  fenceState.externAppFence = 0;
  fn.vkDestroyFence(externDevice, fenceState.externFence, nullptr);
  fenceState.externFence = 0;
}

void ExternalSyncState::setFenceSignaled(VkFence fence) {
  auto fn = spy->mImports.mVkDeviceFunctions[externDevice];
  VkSubmitInfo submitInfo(spy->arena());
  submitInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_SUBMIT_INFO;
  fn.vkQueueSubmit(fenceQueue, 1, &submitInfo, fence);
  while(fn.vkGetFenceStatus(externDevice, fence) != VkResult::VK_SUCCESS) {
    // spin
  }
}

uint32_t ExternalSyncState::waitForFences(CallObserver *observer, uint32_t fenceCount, const VkFence *pFences, VkBool32 waitAll, uint64_t timeout) {
  std::vector<VkFence> externFences;
  for(uint32_t i=0; i<fenceCount; ++i) {
    auto it = fences.find(pFences[i]);
    if(it != fences.end()) {
      externFences.push_back(it->second.externFence);
    }
  }
  auto fn = spy->mImports.mVkDeviceFunctions[externDevice];
  spy->unlock();
  if(externFences.empty()) {
    uint32_t res = fn.vkWaitForFences(appDevice, fenceCount, pFences, waitAll, timeout);
    spy->lock();
    return res;
  }
  std::atomic<bool> waitingExternFences(true);
  std::thread externFencesThread([&]() {
    fn.vkWaitForFences(externDevice, (uint32_t)externFences.size(), externFences.data(), waitAll, timeout);
    if(waitingExternFences.exchange(false)) {
      spy->lock();
      updateFences(observer);
      spy->unlock();
    }
  });
  uint32_t res = fn.vkWaitForFences(appDevice, fenceCount, pFences, waitAll, timeout);
  if(res == VkResult::VK_SUCCESS && waitingExternFences.exchange(false)) {
    spy->lock();
    return res;
  } else {
    externFencesThread.join();
    spy->lock();
    updateFences(observer);
    for(uint32_t i=0; i<fenceCount; ++i) {
      VkFence fence = pFences[i];
      ExternalFenceStatus status;
      auto it = fences.find(fence);
      if(it != fences.end()) {
        status = it->second.status;
      } else {
        status = (ExternalFenceStatus)fn.vkGetFenceStatus(appDevice, fence);
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

void ExternalSyncState::updateFences(CallObserver *observer) {
  auto fn = spy->mImports.mVkDeviceFunctions[externDevice];
  for (auto &p: fences) {
    auto &e = p.second;
    ExternalFenceStatus externFenceStatus = (ExternalFenceStatus)fn.vkGetFenceStatus(externDevice, e.externFence);
    ExternalFenceStatus appFenceStatus = (ExternalFenceStatus)fn.vkGetFenceStatus(externDevice, e.externAppFence);
    switch(e.status) {
      case ExternalFenceStatus::Signaled:
      if (appFenceStatus == ExternalFenceStatus::Unsignaled && externFenceStatus == ExternalFenceStatus::Signaled) {
        // fence reset by application
        // nothing to send, since the vkResetFences call will be sent
        fn.vkResetFences(externDevice, 1, &e.externFence);
      } else if (appFenceStatus == ExternalFenceStatus::Signaled && externFenceStatus == ExternalFenceStatus::Unsignaled) {
        // fence reset externally
        // send a message recording this
        sendFenceReset(e.appFence, observer);
        fn.vkResetFences(externDevice, 1, &e.externAppFence);
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

void ExternalSyncState::sendFenceSignaled(VkFence fence, SignalSource source, CallObserver *observer) {
  auto extra = new vulkan_pb::FenceUpdate();
  extra->set_fence((uint64_t)fence);
  extra->set_source((vulkan_pb::SignalSource)source);
  extra->set_status(vulkan_pb::FenceStatus::SIGNALED);
  observer->encodeAndDelete(extra);
}

void ExternalSyncState::sendFenceReset(VkFence fence, CallObserver *observer) {
  auto extra = new vulkan_pb::FenceUpdate();
  extra->set_fence((uint64_t)fence);
  extra->set_source(vulkan_pb::SignalSource::EXTERNAL);
  extra->set_status(vulkan_pb::FenceStatus::UNSIGNALED);
  observer->encodeAndDelete(extra);
}

// TODO: call this from vkCreateFence
uint32_t ExternalSyncState::createExportFence(VkFence appFence, const VkExportFenceCreateInfo &exportInfo) {
  auto appDevFn = spy->mImports.mVkDeviceFunctions[appDevice];
  auto extDevFn = spy->mImports.mVkDeviceFunctions[externDevice];
  ExternalFenceState fenceState;
  fenceState.isPermanent = true;
  fenceState.appFence = appFence;
  fenceState.status = (ExternalFenceStatus)appDevFn.vkGetFenceStatus(appDevice, appFence);
  uint32_t res;
  {
    VkExportFenceCreateInfo exportInfoClone = exportInfo;
    exportInfoClone.mpNext = nullptr;
    VkFenceCreateInfo createInfo(spy->arena());
    createInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_FENCE_CREATE_INFO;
    createInfo.mpNext = &exportInfoClone;
    res = extDevFn.vkCreateFence(externDevice, &createInfo, nullptr, &fenceState.externFence);
    if(res != VkResult::VK_SUCCESS)
      return res;
  }
  if((exportInfo.mhandleTypes&VkExternalFenceHandleTypeFlagBits::VK_EXTERNAL_FENCE_HANDLE_TYPE_OPAQUE_FD_BIT) != 0) {
    VkFenceGetFdInfoKHR fdInfo(spy->arena());
    fdInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_FENCE_GET_FD_INFO_KHR;
    fdInfo.mfence = appFence;
    fdInfo.mhandleType = VkExternalFenceHandleTypeFlagBits::VK_EXTERNAL_FENCE_HANDLE_TYPE_OPAQUE_FD_BIT;
    int fd;
    res = appDevFn.vkGetFenceFdKHR(appDevice, &fdInfo, &fd);
    if(res != VkResult::VK_SUCCESS)
      return res;

    VkFenceCreateInfo createInfo(spy->arena());
    createInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_FENCE_CREATE_INFO;
    res = extDevFn.vkCreateFence(externDevice, &createInfo, nullptr, &fenceState.externAppFence);
    if(res != VkResult::VK_SUCCESS)
      return res;

    VkImportFenceFdInfoKHR importInfo(spy->arena());
    importInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_IMPORT_FENCE_FD_INFO_KHR;
    importInfo.mfence = fenceState.externAppFence;
    importInfo.mhandleType = VkExternalFenceHandleTypeFlagBits::VK_EXTERNAL_FENCE_HANDLE_TYPE_OPAQUE_FD_BIT;
    importInfo.mfd = fd;
    res = extDevFn.vkImportFenceFdKHR(externDevice, &importInfo);
    if (res != VkResult::VK_SUCCESS)
      return res;
  } else {
    GAPID_ERROR("Unknown external fence handle type (%d)", exportInfo.mhandleTypes);
    return VkResult::VK_ERROR_FEATURE_NOT_PRESENT;
  }
  fences.emplace(appFence, fenceState);
  return VkResult::VK_SUCCESS;
}

uint32_t ExternalSyncState::getFenceFd(const VkFenceGetFdInfoKHR &info, int *fd) {
  VkFenceGetFdInfoKHR externInfo(info);
  auto fenceIt = fences.find(info.mfence);
  if(fenceIt == fences.end()) {
    // This fence should have been created with `ExportFenceCreateInfo`, which should have created an entry in `fences`, so this should not happen under valid usage.
    GAPID_ERROR("getFenceFd called for unknown fence");
    return VkResult::VK_ERROR_VALIDATION_FAILED_EXT;
  }
  externInfo.mfence = fenceIt->second.externFence;
  auto fn = spy->mImports.mVkDeviceFunctions[externDevice];
  return fn.vkGetFenceFdKHR(externDevice, &externInfo, fd);
}

uint32_t ExternalSyncState::importFenceFd(const VkImportFenceFdInfoKHR &importInfo) {
  if(importInfo.mhandleType != VkExternalFenceHandleTypeFlagBits::VK_EXTERNAL_FENCE_HANDLE_TYPE_OPAQUE_FD_BIT) {
    GAPID_ERROR("importFenceFd called with unknown fence handle type %d", importInfo.mhandleType);
    return VkResult::VK_ERROR_VALIDATION_FAILED_EXT;
  }
  uint32_t res;
  auto appDevFn = spy->mImports.mVkDeviceFunctions[appDevice];
  auto extDevFn = spy->mImports.mVkDeviceFunctions[externDevice];
  ExternalFenceState& fenceState = fences[importInfo.mfence];
  if(fenceState.appFence == 0)
    fenceState.appFence = importInfo.mfence;
  //auto fenceIt = fences.find(importInfo.mfence);
  bool isPermanent = (importInfo.mflags & VkFenceImportFlagBits::VK_FENCE_IMPORT_TEMPORARY_BIT) == 0;
  if(isPermanent && !fenceState.isPermanent && fenceState.externAppFence != 0) {
    // Previously this fence had only been imported in temporary mode, but now importod as permanent.
    // Reset `externAppFence` so that we re-import it in permanent mode
    extDevFn.vkDestroyFence(externDevice, fenceState.externAppFence, nullptr);
    fenceState.externAppFence = 0;
  }
  fenceState.isPermanent |= isPermanent;
  if(fenceState.externAppFence == 0) {
    VkExportFenceCreateInfo exportInfo(spy->arena());
    exportInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_EXPORT_FENCE_CREATE_INFO;
    exportInfo.mhandleTypes = importInfo.mhandleType;

    VkFenceCreateInfo createInfo(spy->arena());
    createInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_FENCE_CREATE_INFO;
    createInfo.mpNext = &exportInfo;

    res = extDevFn.vkCreateFence(externDevice, &createInfo, nullptr, &fenceState.externAppFence);
    if(res != VkResult::VK_SUCCESS)
      return res;

    VkFenceGetFdInfoKHR fdInfo(spy->arena());
    fdInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_FENCE_GET_FD_INFO_KHR;
    fdInfo.mfence = fenceState.externAppFence;
    fdInfo.mhandleType = VkExternalFenceHandleTypeFlagBits::VK_EXTERNAL_FENCE_HANDLE_TYPE_OPAQUE_FD_BIT;
    int fd;
    res = extDevFn.vkGetFenceFdKHR(externDevice, &fdInfo, &fd);
    if(res != VkResult::VK_SUCCESS)
      return res;

    VkImportFenceFdInfoKHR appImport(importInfo);
    appImport.mfd = fd;
    res = appDevFn.vkImportFenceFdKHR(appDevice, &appImport);
    if(res != VkResult::VK_SUCCESS)
      return res;
  }
  if(fenceState.externFence == 0) {
    VkFenceCreateInfo createInfo(spy->arena());
    createInfo.msType = VkStructureType::VK_STRUCTURE_TYPE_FENCE_CREATE_INFO;
    res = extDevFn.vkCreateFence(externDevice, &createInfo, nullptr, &fenceState.externFence);
    if(res != VkResult::VK_SUCCESS)
      return res;
  }
  VkImportFenceFdInfoKHR extImport(importInfo);
  extImport.mfence = fenceState.externFence;
  res = extDevFn.vkImportFenceFdKHR(externDevice, &extImport);
  if(res != VkResult::VK_SUCCESS)
    return res;
}

uint32_t ExternalSyncState::resetFences(uint32_t fenceCount, const VkFence* pFences) {
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
    auto appDevFn = spy->mImports.mVkDeviceFunctions[appDevice];
    uint32_t r = appDevFn.vkResetFences(appDevice, (uint32_t)appFences.size(), appFences.data());
    if(r != VkResult::VK_SUCCESS)
      res = r;
  }
  if(!externFences.empty()) {
    auto extDevFn = spy->mImports.mVkDeviceFunctions[externDevice];
    uint32_t r = extDevFn.vkResetFences(externDevice, (uint32_t)externFences.size(), externFences.data());
    if(r != VkResult::VK_SUCCESS)
      res = r;
  }
  for(auto fenceIt: destroy) {
    destroyFenceState(fenceIt->second);
  }
  return res;
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