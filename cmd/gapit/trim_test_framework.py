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

import argparse
import collections
from datetime import datetime
from multiprocessing import Pool
import os
import string
import subprocess

SUCCESS = 0
FAILURE = 1
WARNING = 2
SKIPPED = 3

timeout = 120

class TrimTestException(Exception):
  def __init__(self, message):
    self.message = message
  def __str__(self):
    return self.message

FrameRange = collections.namedtuple('FrameRange', 'start count')
TrimSpec = collections.namedtuple('TrimSpec', 'range checks')
TrimTest = collections.namedtuple('TrimTest', 'app_name trace_range trim_specs config')

def run(args, log_path):
  with open(log_path, "w") as log_file:
    log_file.write("+ {}".format(" ".join(args)))
    res = subprocess.run(args, timeout=timeout, stdout=log_file, stderr=subprocess.STDOUT)
    res.check_returncode()

class TrimTestManager(object):
  def __init__(self, gapid_dir, bin_dir, trace_dir, threads=1):
    if trace_dir is None:
      time = datetime.now().strftime("%Y%m%d_%H%M%S")
      trace_dir = os.path.join(bin_dir, 'trace_{}'.format(time))
    trace_dir = os.path.realpath(trace_dir)
    try:
      os.makedirs(trace_dir)
    except FileExistsError:
      pass

    self.gapid_dir = gapid_dir
    self.bin_dir = bin_dir
    self.trace_dir = trace_dir
    self.default_screenshot_path = os.path.join(self.bin_dir, "screenshot.png")
    self.default_effects_path = os.path.join(self.bin_dir, "temp.effects")
    self.threads = threads
    self.tests = []
    self.number_of_tests_passed = 0
    self.failed_tests = []

  def base_name(self, app_name, trace_range, trim_range=None, frame=None):
    trace_start, trace_count = trace_range
    name = app_name
    if trace_start is not None:
      trace_end = trace_start + trace_count - 1
      name = "{}.{}-{}".format(app_name, trace_start, trace_end)
    else:
      name = "{}.{}".format(app_name, trace_count)

    if trim_range is not None:
      trim_start, trim_count = trim_range
      if trim_count > 1:
        trim_end = trim_start + trim_count - 1
        name = "{}.{}-{}".format(name, trim_start, trim_end)
      else:
        name = "{}.{}".format(name, trim_start)

    if frame is not None:
      name = "{}.{}".format(name, frame)

    return name

  def print_status(self, status, status_align, app, trace_range, trim_range, frame):
    status_justify = 15
    if status_align < 0:
      status = status.ljust(status_justify)
    elif status_align > 0:
      status = status.rjust(status_justify)
    else:
      status = status.center(status_justify)
    if trace_range is None:
      trace_range = ""
    else:
      if trace_range.start is None or trace_range == 0:
        trace = str(trace_range.count)
      else:
        trace = "({},{})".format(trace_range.start, trace_range.start + trace_range.count - 1)
    if trim_range is None:
      trim = ""
    else:
      if trim_range.start is None:
        trim = str(trim_range.count)
      else:
        trim = "({},{})".format(trim_range.start, trim_range.start + trim_range.count - 1)
    if frame is None:
      frame = ""
    else:
      frame = str(frame)

    app = app.ljust(20)
    trace = trace.ljust(10)
    trim = trim.ljust(10)
    frame = frame.ljust(10)
    print("[ {} ] {} {} {} {}".format(status, app, trace, trim, frame))

  def trace_path(self, app_name, trace_range, trim_range=None):
    name = self.base_name(app_name, trace_range, trim_range)
    return os.path.join(self.trace_dir, "{}.gfxtrace".format(name))

  def screenshot_path(self, app_name, trace_range, trim_range, frame):
    name = self.base_name(app_name, trace_range, trim_range, frame)
    return os.path.join(self.trace_dir, "{}.png".format(name ))

  def screenshot_diff_path(self, app_name, trace_range, trim_range, frame):
    name = self.base_name(app_name, trace_range, trim_range, frame)
    return os.path.join(self.trace_dir, "DIFF-{}.png".format(name))

  def effects_path(self, app_name, trace_range, trim_range):
    name = self.base_name(app_name, trace_range, trim_range)
    return os.path.join(self.trace_dir, "{}.effects".format(name ))

  def log_path(self, cmd, app_name, trace_range, trim_range=None, frame=None):
    name = self.base_name(app_name, trace_range, trim_range, frame)
    return os.path.join(self.trace_dir, "{}-{}.log".format(cmd, name))

  def gapit_path(self):
    return os.path.join(self.gapid_dir, 'gapit')

  def run_gapit(self, verb, args, log_path):
    gapit_args = [self.gapit_path(), "-log-level", "Debug", verb]
    gapit_args.extend(args)
    run(gapit_args, log_path)

  def do_trace(self, app_name, trace_range, config):
    self.print_status("TRACE", -1, app_name, trace_range, None, None)
    trace_start, trace_count = trace_range
    trace_path = self.trace_path(app_name, trace_range)
    trace_args = ['-local-app', config.get('bin', app_name),
                  '-local-workingdir', os.path.realpath(config.get('dir', '.')),
                  '-capture-frames', str(trace_count),
                  '-out', trace_path]
    if trace_start is not None:
      trace_args.extend(['-start-at-frame', str(trace_start)])
    try:
      self.run_gapit('trace', trace_args, self.log_path('trace', app_name, trace_range))
    except subprocess.TimeoutExpired:
      raise TrimTestException("Timeout while capturing frames {} from {}".format(trace_range, app_name))
    except subprocess.CalledProcessError:
      raise TrimTestException("Error while capturing frames {} from {}".format(trace_range, app_name))
    if not os.path.exists(trace_path):
      raise TrimTestException("Could not find capture {}".format(trace_path))

  def do_trim(self, app_name, trace_range, trim_range):
    self.print_status("TRIM", -1, app_name, trace_range, trim_range, None)
    trim_start, trim_count = trim_range
    trace_path = self.trace_path(app_name, trace_range)
    trim_path = self.trace_path(app_name, trace_range, trim_range)
    trim_args = ['-frames-start', str(trim_start),
                 '-frames-count', str(trim_count),
                 '-out', trim_path,
                 trace_path]
    try:
      self.run_gapit('trim', trim_args, self.log_path('trim', app_name, trace_range, trim_range))
    except subprocess.TimeoutExpired:
      raise TrimTestException("Timeout while trimming frames {} from {}".format(trim_range, trace_path))
    except subprocess.CalledProcessError:
      raise TrimTestException("Error while trimming frames {} from {}".format(trim_range, trace_path))
    if not os.path.exists(trim_path):
      raise TrimTestException("Could not find trimmed capture {}".format(trim_path))
    try:
      os.rename(self.default_effects_path, self.effects_path(app_name, trace_range, trim_range))
    except:
      pass

  def do_screenshot(self, app_name, trace_range, trim_range, frame):
    self.print_status("SCREENSHOT", -1, app_name, trace_range, trim_range, frame)
    screenshot_path = self.screenshot_path(app_name, trace_range, trim_range, frame)
    screenshot_args = ['-frame', str(frame),
                       '-out', screenshot_path,
                       self.trace_path(app_name, trace_range, trim_range)]
    try:
      self.run_gapit('screenshot', screenshot_args, self.log_path('screenshot', app_name, trace_range, trim_range, frame))
    except subprocess.TimeoutExpired:
      raise TrimTestException("Timeout getting screenshot of frame {} from {}".format(frame, self.trace_path(app_name, trace_range, trim_range)))
    except subprocess.CalledProcessError:
      raise TrimTestException("Error getting screenshot of frame {} from {}".format(frame, self.trace_path(app_name, trace_range, trim_range)))
    if not os.path.exists(screenshot_path):
      raise TrimTestException("Could not find screenshot file {}".format(screenshot_path))
    convert_args = ['convert', screenshot_path, '-alpha', 'off', screenshot_path]
    subprocess.run(convert_args)

  def check_screenshot_imgdiff(self, app_name, trace_range, trim_range, frame, config):
    self.print_status("CHECK", -1, app_name, trace_range, trim_range, frame)
    trim_start, _ = trim_range
    orig_screenshot = self.screenshot_path(app_name, trace_range, None, trim_start + frame)
    trim_screenshot = self.screenshot_path(app_name, trace_range, trim_range, frame)
    diff_path = self.screenshot_diff_path(app_name, trace_range, trim_range, frame)

    imgdiff_args = ['imgdiff', '-a', 'perceptual', '-o', diff_path, orig_screenshot, trim_screenshot]
    imgdiff_proc = subprocess.run(imgdiff_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if imgdiff_proc.returncode != 0:
      pixel_ratio = float(imgdiff_proc.stdout.decode("utf-8").strip().split()[3].strip('%'))
      if pixel_ratio > config.get('badPixelRatio', 0):
        raise TrimTestException("Screenshots differ ({})  {}  {}".format(pixel_ratio, os.path.basename(orig_screenshot), os.path.basename(trim_screenshot)))

  def check_screenshot(self, app_name, trace_range, trim_range, frame, config):
    self.print_status("CHECK", -1, app_name, trace_range, trim_range, frame)
    trim_start, _ = trim_range
    orig_screenshot = self.screenshot_path(app_name, trace_range, None, trim_start + frame)
    trim_screenshot = self.screenshot_path(app_name, trace_range, trim_range, frame)
    diff_path = self.screenshot_diff_path(app_name, trace_range, trim_range, frame)

    fuzz = config.get('fuzz', 0)
    fuzz = '{}%'.format(fuzz*100)

    compare_args = ['compare', '-fuzz', fuzz, '-metric', 'AE', orig_screenshot, trim_screenshot, diff_path]
    error = TrimTestException("Screenshots differ:\n    {}\n    {}".format( orig_screenshot, trim_screenshot))
    compare_proc = subprocess.run(compare_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    num_bad_pixels = int(compare_proc.stdout)
    if num_bad_pixels > config.get('numBadPixels', 0):
      raise error

  def add_test(self, app_name, trace_range, trims, config):
    self.tests.append(TrimTest(app_name, trace_range, trims, config))

  def run_tests(self):
    header = "{} {} {} {} {}".format(" "*19, "App".ljust(20), "Trace".ljust(10), "Trim".ljust(10), "Frame".ljust(5))
    print(header)
    print("-"*len(header))
    os.chdir(self.bin_dir)
    for test in self.tests:
      for res in self.run_test(test):
        self.report_test(res)

  def print_summary_and_return_code(self):
    print("Total Tests Run: {}".format(self.number_of_tests_passed + len(self.failed_tests)))
    print("Total Tests Passed: {}".format(self.number_of_tests_passed))
    for result in self.failed_tests:
      self.print_status("FAILED", 1, result['app_name'], result['trace_range'], result.get('trim_range'), result.get('frame'))
      print("     " + result['error'])
    if self.failed_tests:
      return -1
    return 0

  def report_test(self, result):
    if 'error' in result:
      status = "FAILED"
    else:
      status = "OK"
    self.print_status(status, 1, result['app_name'], result['trace_range'], result.get('trim_range'), result.get('frame'))
    if 'error' in result:
      print("    {}".format(result['error']))
      self.failed_tests.append(result)
    else:
      self.number_of_tests_passed += 1

  def run_test(self, test_spec):
    app_name, trace_range, trims, config = test_spec
    if config is None:
      config = {}
    if config.get('skip', False):
      return
    result = {'app_name': app_name,
               'trace_range': trace_range}
    def error_result(error):
      result_copy = result.copy()
      result_copy['error'] = error.message
      return result_copy
    try:
      self.do_trace(app_name, trace_range, config)
    except TrimTestException as error:
      yield error_result(error)
      return
    for t in trims:
      trim_range, trim_checks = t
      trim_start, _ = trim_range
      if trim_start > 0 and config.get('nocut', False):
        continue
      result['trim_range'] = trim_range
      try:
        self.do_trim(app_name, trace_range, trim_range)
      except TrimTestException as error:
        yield error_result(error)
        continue
      for frame in trim_checks:
        result['frame'] = frame
        try:
          self.do_screenshot(app_name, trace_range, None, trim_start + frame)
          self.do_screenshot(app_name, trace_range, trim_range, frame)
          self.check_screenshot(app_name, trace_range, trim_range, frame, config)
        except TrimTestException as error:
          yield error_result(error)
          continue
        yield result.copy()
      result.pop('frame')

def main(suite_apps, trim_specs):
  parser = argparse.ArgumentParser(description='''
  This testing program will capture vulkan test applications, trim the captures, and verify the trimmed captures.
      ''')
  parser.add_argument('--bin_dir', nargs=1, help="Directory containing the test executables")
  parser.add_argument('--gapid_dir', nargs=1, help="Directory containing the gapid executables.")
  parser.add_argument('--trace_dir', nargs=1, help="")
  parser.add_argument('--exclude', action='append', help="space separated list of apps in the suite to exclude")
  parser.add_argument('--apps', action='append', help="space separated list of apps in the suite to include")
  args = parser.parse_args()

  if args.gapid_dir is not None:
    gapid_dir = args.gapid_dir[0]
  else:
    script_dir = os.path.dirname(os.path.realpath(__file__))
    gapid_dir = os.path.join(script_dir, '../../bazel-bin/pkg/')
  gapid_dir = os.path.realpath(gapid_dir)

  if args.bin_dir is None:
    print("Please specify the directory containing the test executables (--bin_dir).")
    exit(-1)
  bin_dir = args.bin_dir[0]

  apps = {}
  if args.apps is not None:
    for app_space_sep in args.apps:
      for app in app_space_sep.split():
        apps[app] = suite_apps.get(app)
  else:
    exclude = set()
    if args.exclude is not None:
      for excluded_space_sep in args.exclude:
        for a in excluded_space_sep.split():
          exclude.add(a)
    for a, config in suite_apps.items():
      if a not in exclude:
        apps[a] = config

  trace_dir = None
  if args.trace_dir is not None:
    trace_dir = args.trace_dir[0]

  manager = TrimTestManager(gapid_dir, bin_dir, trace_dir)
  for app_name in sorted(apps.keys()):
    config = apps[app_name]
    for trace_range, trim_spec in trim_specs.items():
      manager.add_test(app_name, trace_range, trim_spec, config)

  manager.run_tests()
  return manager.print_summary_and_return_code()
