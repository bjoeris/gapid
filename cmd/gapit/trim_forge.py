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


import os
from trim_test_framework import FrameRange, TrimSpec, main

def forge_config(name, **kwargs):
  config = {}
  for key, value in kwargs.items():
    config[key] = value
  config['dir'] = os.path.join(name, 'Debug')
  config['bin'] = os.path.join(name, 'Debug', name)
  return config

apps = {
    '01_Transformation': forge_config('01_Transformation', nocut=True),
    '02_Compute': forge_config('02_Compute'),
    '03_MultiThread': forge_config('03_MultiThread'),
    '04_ExecuteIndirect': forge_config('04_ExecuteIndirect'),
    '05_FontRendering': forge_config('05_FontRendering'),
    '06_BRDF': forge_config('06_BRDF'),
    '07_Tessellation': forge_config('07_Tessellation'),
    '08_Procedural': forge_config('08_Procedural'),
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
