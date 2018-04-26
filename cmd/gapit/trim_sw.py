#!/usr/bin/python3
# Copyright (C) 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from trim_test_framework import FrameRange, TrimSpec, main

apps = {
    'bloom': None,
    'computecloth': None,
    'computecullandlod': None,
    # 'computeheadless',  # no gfx
    'computenbody': {'badPixelRatio': 5e-4, 'nocut': True},
    'computeparticles': {'badPixelRatio': 5e-4, 'nocut': True},
    'computeshader': None,
    # 'conservativeraster',  # extension not supported on my workstation
    'debugmarker': None,
    'deferred': None,
    'deferredmultisampling': {'fuzz': 0.02},
    'deferredshadows': None,
    'displacement': {'fuzz': 0.02, 'numBadPixels': 10},
    'distancefieldfonts': None,
    'dynamicuniformbuffer': None,
    'gears': None,
    'geometryshader': None,
    'hdr': None,
    'imgui': None,
    'indirectdraw': None,
    'instancing': None,
    'mesh': None,
    'multisampling': None,
    'multithreading': None,
    'occlusionquery': None,
    'offscreen': None,
    'parallaxmapping': None,
    'particlefire': {'fuzz': 0.02, 'numBadPixels': 10},
    'pbrbasic': None,
    'pbribl': {'skip': True},
    'pbrtexture': {'skip': True},
    'pipelines': None,
    'pipelinestatistics': None,
    'pushconstants': None,
    'pushdescriptors': None,
    'radialblur': None,
    'raytracing': {'skip': True},
    # 'renderheadless',  # no gfx
    'scenerendering': {'fuzz': 0.05, 'numBadPixels': 250, 'badPixelRatio': 2e-4},
    'screenshot': None,
    'shadowmapping': None,
    'shadowmappingcascade': None,
    'shadowmappingomni': None,
    'skeletalanimation': None,
    'specializationconstants': None,
    'sphericalenvmapping': None,
    'ssao': None,
    'stencilbuffer': None,
    'subpasses': None,
    'terraintessellation': {'fuzz': 0.01, 'numBadPixels': 5},
    'tessellation': None,
    'textoverlay': None,
    'texture': None,
    'texture3d': None,
    'texturearray': None,
    'texturecubemap': {'fuzz': 0.01, 'numBadPixels': 5},
    'texturemipmapgen': {'nocut': True},
    'texturesparseresidency': {'skip': True},
    'triangle': None,
    'viewportarray': None,
    'vulkanscene': None,
 }

trim_specs = {
    FrameRange(None, 20): [
        TrimSpec(FrameRange(0, 1), [0]),
        TrimSpec(FrameRange(5, 1), [0]),
        TrimSpec(FrameRange(10, 1), [0]),
        TrimSpec(FrameRange(0, 20), [0, 1, 5, 10, 19]),
        TrimSpec(FrameRange(5, 10), [0, 1, 5, 9])],
    FrameRange(100, 20): [
        TrimSpec(FrameRange(0, 1), [0]),
        TrimSpec(FrameRange(5, 1), [0]),
        TrimSpec(FrameRange(10, 1), [0]),
        TrimSpec(FrameRange(0, 20), [0, 1, 5, 10, 19]),
        TrimSpec(FrameRange(5, 10), [0, 1, 5, 9])],
}

if __name__ == "__main__":
  main(apps, trim_specs)
