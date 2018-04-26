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
    'async_compute': {'nocut': True},
    'blend_constants': None,
    'blit_image': None,
    'bufferview': None,
    'clear_attachments': None,
    'clear_colorimage': None,
    'clear_depthimage': None,
    'copy_image_buffer': None,
    'copy_querypool_results': {'nocut': True, 'skip': True},
    'cube': None,
    'depth_bounds': None,
    'depth_readback': {'nocut': True},
    'dispatch': None,
    'dispatch_indirect': {'skip': True, 'nocut': True},
    'execute_commands': None,
    'fill_buffer': None,
    # 'passthrough',     # bad trace
    'set_event': None,
    # 'simple_compute',  # bad trace
    # 'sparse_binding',  # bad trace
    'stencil': None,
    'textured_cube': None,
    'wireframe': None,
    'write_timestamp': None,
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
