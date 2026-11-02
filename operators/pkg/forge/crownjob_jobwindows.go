// Copyright 2020-2026 Politecnico di Torino
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package forge

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	clv1alpha1 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha1"
)

const (
	// JobWindowHorizon -> how far ahead the job windows are looked at, to find when the limits in force change next.
	// The windows repeat every week, so a day more than a week covers a window that ends at "24:00".
	JobWindowHorizon = 8 * 24 * time.Hour

	minutesPerDay = 24 * 60
	// jobWindowZoneProbe -> how far from a wall-clock time the offsets of its time zone are looked up: more than the
	// largest daylight saving change.
	jobWindowZoneProbe = 3 * time.Hour
)

// jobWeekDays maps the days of the week of Go to those of the job windows.
var jobWeekDays = map[time.Weekday]clv1alpha1.WeekDay{
	time.Monday: clv1alpha1.WeekDayMonday, time.Tuesday: clv1alpha1.WeekDayTuesday,
	time.Wednesday: clv1alpha1.WeekDayWednesday, time.Thursday: clv1alpha1.WeekDayThursday,
	time.Friday: clv1alpha1.WeekDayFriday, time.Saturday: clv1alpha1.WeekDaySaturday, time.Sunday: clv1alpha1.WeekDaySunday,
}

// JobTimeZone returns the time zone in which the job windows of a Workspace are read: the one of its jobPolicy, or
// else the given default. An error is returned if the name is not a valid IANA time zone ("Local", the time zone of
// the operator, is not one).
func JobTimeZone(policy *clv1alpha1.JobPolicy, defaultZone *time.Location) (*time.Location, error) {
	if policy == nil || policy.TimeZone == "" {
		return defaultZone, nil
	}
	if policy.TimeZone == "Local" {
		return nil, fmt.Errorf("unknown time zone %s", policy.TimeZone)
	}
	return time.LoadLocation(policy.TimeZone)
}

// JobWindowMinute returns the minute of the day of a time of a job window, written "HH:MM" ("24:00" is the end of the day).
func JobWindowMinute(value string) (int, error) {
	hours, minutes, found := strings.Cut(value, ":")
	h, errH := strconv.Atoi(hours)
	m, errM := strconv.Atoi(minutes)
	if !found || errH != nil || errM != nil || h < 0 || m < 0 || m > 59 || h*60+m > minutesPerDay {
		return 0, fmt.Errorf("invalid time of the day %q", value)
	}
	return h*60 + m, nil
}

// JobWindowCovers returns whether the given window covers the instant t, read on the wall clock of loc: the day of the
// week of t is one of the days of the window, and its time of the day is in [start, end). A window whose start or end
// cannot be read covers nothing.
func JobWindowCovers(window *clv1alpha1.JobWindow, loc *time.Location, t time.Time) bool {
	start, errStart := JobWindowMinute(window.Start)
	end, errEnd := JobWindowMinute(window.End)
	if errStart != nil || errEnd != nil {
		return false
	}
	local := t.In(loc)
	minute := local.Hour()*60 + local.Minute()
	return jobWindowHasDay(window, local.Weekday()) && minute >= start && minute < end
}

// JobWindowLimits returns the limits in force during the given window: the defaults, with each field set in the
// overrides of the window replacing the default one as a whole.
func JobWindowLimits(defaults *clv1alpha1.QueueLimits, window *clv1alpha1.JobWindow) clv1alpha1.QueueLimits {
	limits := clv1alpha1.QueueLimits{}
	if defaults != nil {
		limits = *defaults.DeepCopy()
	}
	overrides := window.Overrides.DeepCopy()
	if overrides.MaxAtOnce != nil {
		limits.MaxAtOnce = overrides.MaxAtOnce
	}
	if overrides.Guaranteed != nil {
		limits.Guaranteed = overrides.Guaranteed
	}
	if overrides.MaxRunTime != "" {
		limits.MaxRunTime = overrides.MaxRunTime
	}
	if overrides.PerUser != nil {
		limits.PerUser = overrides.PerUser
	}
	return limits
}

// JobLimitsAt returns the limits of the job queue of a Workspace in force at the instant t: the defaults
// (spec.queueLimits), changed by the first window, in list order, that covers t. Windows are never merged with each
// other. A nil loc (the time zone of the Workspace is not valid) ignores the windows.
func JobLimitsAt(spec *clv1alpha1.WorkspaceSpec, loc *time.Location, t time.Time) clv1alpha1.QueueLimits {
	if loc != nil {
		for i := range spec.Windows {
			if JobWindowCovers(&spec.Windows[i], loc, t) {
				return JobWindowLimits(spec.QueueLimits, &spec.Windows[i])
			}
		}
	}
	if spec.QueueLimits == nil {
		return clv1alpha1.QueueLimits{}
	}
	return *spec.QueueLimits.DeepCopy()
}

// JobLimitsAll returns every set of limits a Workspace can have in force: the defaults, then those of each window.
// A nil loc (the time zone of the Workspace is not valid) returns the defaults only, since the windows never apply.
func JobLimitsAll(spec *clv1alpha1.WorkspaceSpec, loc *time.Location) []clv1alpha1.QueueLimits {
	all := []clv1alpha1.QueueLimits{JobLimitsAt(spec, nil, time.Time{})}
	if loc != nil {
		for i := range spec.Windows {
			all = append(all, JobWindowLimits(spec.QueueLimits, &spec.Windows[i]))
		}
	}
	return all
}

// JobLimitBoundaries returns, in order, the instants strictly after now and within JobWindowHorizon at which a window
// starts or ends, on the wall clock of loc. On a day when the clock is moved forward, a start or an end that the clock
// skips happens when the clock jumps; on a day when it is moved back, a start or an end in the repeated hour happens
// twice, and the jump back is a boundary too (the wall clock moves back across it).
func JobLimitBoundaries(windows []clv1alpha1.JobWindow, loc *time.Location, now time.Time) []time.Time {
	if loc == nil || len(windows) == 0 {
		return nil
	}
	horizon := now.Add(JobWindowHorizon)
	local := now.In(loc)
	found := map[int64]time.Time{}
	for day := 0; day <= int(JobWindowHorizon/(24*time.Hour)); day++ {
		// Noon of the day: its date and day of the week, whatever the daylight saving changes.
		noon := time.Date(local.Year(), local.Month(), local.Day()+day, 12, 0, 0, 0, loc)
		for i := range windows {
			if !jobWindowHasDay(&windows[i], noon.Weekday()) {
				continue
			}
			for _, value := range []string{windows[i].Start, windows[i].End} {
				minute, err := JobWindowMinute(value)
				if err != nil {
					continue
				}
				for _, instant := range jobWallClockInstants(noon, minute, loc) {
					if instant.After(now) && !instant.After(horizon) {
						found[instant.UnixNano()] = instant
					}
				}
			}
		}
	}

	boundaries := make([]time.Time, 0, len(found))
	for _, instant := range found {
		boundaries = append(boundaries, instant.In(loc))
	}
	sort.Slice(boundaries, func(i, j int) bool { return boundaries[i].Before(boundaries[j]) })
	return boundaries
}

// JobNextBoundary returns the first instant strictly after now at which a job window of the Workspace starts or ends,
// and false if there is none within JobWindowHorizon.
func JobNextBoundary(spec *clv1alpha1.WorkspaceSpec, loc *time.Location, now time.Time) (time.Time, bool) {
	boundaries := JobLimitBoundaries(spec.Windows, loc, now)
	if len(boundaries) == 0 {
		return time.Time{}, false
	}
	return boundaries[0], true
}

// JobNextTimeAllowing returns the first instant strictly after now at which the limits in force satisfy allows, and
// false if there is none within JobWindowHorizon. The limits change only at the boundaries of the windows.
func JobNextTimeAllowing(spec *clv1alpha1.WorkspaceSpec, loc *time.Location, now time.Time, allows func(*clv1alpha1.QueueLimits) bool) (time.Time, bool) {
	for _, boundary := range JobLimitBoundaries(spec.Windows, loc, now) {
		limits := JobLimitsAt(spec, loc, boundary)
		if allows(&limits) {
			return boundary, true
		}
	}
	return time.Time{}, false
}

// FormatJobWindowTime returns an instant as shown to the user: the day of the week and the time, on the wall clock of
// loc (e.g., "Mon 20:00").
func FormatJobWindowTime(t time.Time, loc *time.Location) string {
	return t.In(loc).Format("Mon 15:04")
}

// jobWindowHasDay returns whether the given day of the week is one of the days of the window.
func jobWindowHasDay(window *clv1alpha1.JobWindow, day time.Weekday) bool {
	for _, d := range window.Days {
		if d == jobWeekDays[day] {
			return true
		}
	}
	return false
}

// jobWallClockInstants returns the instants at which the wall clock of loc crosses the given minute of the day of
// noon (1440 is midnight at its end): one on most days; two when the clock is moved back across it, with the jump
// back in between; the jump forward when the clock skips it.
func jobWallClockInstants(noon time.Time, minute int, loc *time.Location) []time.Time {
	y, m, d := noon.Date()
	asUTC := time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Add(time.Duration(minute) * time.Minute)
	guess := time.Date(y, m, d, 0, minute, 0, 0, loc)

	// The instants at which the clock shows the minute, under each offset in force around it.
	found := map[int64]time.Time{}
	for _, probe := range []time.Time{guess.Add(-jobWindowZoneProbe), guess, guess.Add(jobWindowZoneProbe)} {
		_, offset := probe.Zone()
		instant := asUTC.Add(-time.Duration(offset) * time.Second)
		if sameWallClock(instant.In(loc), asUTC) {
			found[instant.UnixNano()] = instant
		}
	}
	instants := make([]time.Time, 0, len(found))
	for _, instant := range found {
		instants = append(instants, instant)
	}
	sort.Slice(instants, func(i, j int) bool { return instants[i].Before(instants[j]) })

	switch len(instants) {
	case 0:
		// Skipped by a jump forward: the clock passes the minute when the zone of the guess starts or ends.
		start, end := guess.ZoneBounds()
		if !start.IsZero() && guess.Sub(start) <= jobWindowZoneProbe {
			return []time.Time{start}
		}
		return []time.Time{end}
	case 2:
		// Repeated by a jump back: the clock also moves back across the minute when the zone of the first one ends.
		_, end := instants[0].In(loc).ZoneBounds()
		return []time.Time{instants[0], end, instants[1]}
	default:
		return instants
	}
}

// sameWallClock returns whether the given local time shows the date and time of the reference written in UTC.
func sameWallClock(local, reference time.Time) bool {
	ly, lm, ld := local.Date()
	ry, rm, rd := reference.Date()
	return ly == ry && lm == rm && ld == rd && local.Hour() == reference.Hour() &&
		local.Minute() == reference.Minute() && local.Second() == 0 && local.Nanosecond() == 0
}
