// Copyright (C) 2018 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"io/ioutil"
	"path/filepath"

	"github.com/google/gapid/core/app"
	"github.com/google/gapid/core/log"
	"github.com/google/gapid/gapis/service"
	"github.com/google/gapid/gapis/service/path"
)

type trimVerb struct{ TrimFlags }

func init() {
	verb := &trimVerb{}
	verb.Frames.Count = allTheWay

	app.AddVerb(&app.Verb{
		Name:      "trim",
		ShortHelp: "Trims a gfx trace to the dependencies of the requested frames",
		Action:    verb,
	})
}

func (verb *trimVerb) Run(ctx context.Context, flags flag.FlagSet) error {
	if flags.NArg() != 1 {
		app.Usage(ctx, "Exactly one gfx trace file expected, got %d", flags.NArg())
		return nil
	}

	filepath, err := filepath.Abs(flags.Arg(0))
	if err != nil {
		return log.Errf(ctx, err, "Finding file: %v", flags.Arg(0))
	}

	client, err := getGapis(ctx, verb.Gapis, verb.Gapir)
	if err != nil {
		return log.Err(ctx, err, "Failed to connect to the GAPIS server")
	}
	defer client.Close()

	capture, err := client.LoadCapture(ctx, filepath)
	if err != nil {
		return log.Errf(ctx, err, "LoadCapture(%v)", filepath)
	}

	eofEvents, err := verb.eofEvents(ctx, capture, client)
	if err != nil {
		return err
	}
	begin, count := verb.getRange(eofEvents)

	capture, err = client.TrimCapture(ctx, capture, begin, count)
	if err != nil {
		return log.Errf(ctx, err, "TrimCapture(%v, %v, %v)", capture, begin, count)
	}

	data, err := client.ExportCapture(ctx, capture)
	if err != nil {
		return log.Errf(ctx, err, "ExportCapture(%v)", capture)
	}

	output := verb.Out
	if output == "" {
		output = "trimmed.gfxtrace"
	}
	if err := ioutil.WriteFile(output, data, 0666); err != nil {
		return log.Errf(ctx, err, "Writing file: %v", output)
	}
	return nil
}

func (verb *trimVerb) eofEvents(ctx context.Context, capture *path.Capture, client service.Service) ([]*service.Event, error) {
	filter, err := verb.CommandFilterFlags.commandFilter(ctx, client, capture)
	if err != nil {
		return nil, log.Err(ctx, err, "Couldn't get filter")
	}
	requestEvents := path.Events{
		Capture:     capture,
		LastInFrame: true,
		Filter:      filter,
	}

	if verb.Commands {
		requestEvents.LastInFrame = false
		requestEvents.AllCommands = true
	}

	// Get the end-of-frame events.
	eofEvents, err := getEvents(ctx, client, &requestEvents)
	if err != nil {
		return nil, log.Err(ctx, err, "Couldn't get frame events")
	}

	lastFrame := verb.Frames.Start
	if verb.Frames.Count > 0 {
		lastFrame += verb.Frames.Count - 1
	}
	if lastFrame >= len(eofEvents) {
		return nil, log.Errf(ctx, nil, "Requested frame %d, but capture only contains %d frames", lastFrame, len(eofEvents))
	}

	return eofEvents, nil
}

func (verb *trimVerb) getRange(eofEvents []*service.Event) (start uint64, count int64) {
	start = 0
	if verb.Frames.Start > 0 {
		start = eofEvents[verb.Frames.Start-1].Command.Indices[0] + 1
	}
	if verb.Frames.Count >= 0 {
		end := eofEvents[verb.Frames.Start+verb.Frames.Count-1].Command.Indices[0] + 1
		count = int64(end - start)
	} else {
		end := eofEvents[len(eofEvents)-1].Command.Indices[0] + 1
		count = int64(end - start)
	}
	return
}
