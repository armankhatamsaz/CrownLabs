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

package forge_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"

	clv1alpha1 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha1"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
)

var _ = Describe("Job windows", func() {
	rome, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		panic(err)
	}
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		panic(err)
	}
	at := func(loc *time.Location, value string) time.Time {
		t, err := time.ParseInLocation("2006-01-02 15:04:05", value, loc)
		ExpectWithOffset(1, err).ToNot(HaveOccurred())
		return t
	}
	utc := func(value string) time.Time { return at(time.UTC, value) }
	// instants returns the given times as UTC text, to compare instants whatever their location.
	instants := func(times ...time.Time) []string {
		out := make([]string, 0, len(times))
		for _, t := range times {
			out = append(out, t.UTC().Format(time.RFC3339))
		}
		return out
	}
	window := func(start, end string, days ...clv1alpha1.WeekDay) clv1alpha1.JobWindow {
		return clv1alpha1.JobWindow{Days: days, Start: start, End: end}
	}
	quantities := func(kv ...string) map[string]resource.Quantity {
		m := map[string]resource.Quantity{}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = resource.MustParse(kv[i+1])
		}
		return m
	}
	const mon, tue, sun = clv1alpha1.WeekDayMonday, clv1alpha1.WeekDayTuesday, clv1alpha1.WeekDaySunday

	Describe("The forge.JobTimeZone function", func() {
		It("Should use the zone of the jobPolicy, or else the default, and refuse names that are not IANA zones", func() {
			loc, err := forge.JobTimeZone(&clv1alpha1.JobPolicy{TimeZone: "Europe/Rome"}, time.UTC)
			Expect(err).ToNot(HaveOccurred())
			Expect(loc.String()).To(Equal("Europe/Rome"))
			loc, err = forge.JobTimeZone(&clv1alpha1.JobPolicy{}, rome)
			Expect(err).ToNot(HaveOccurred())
			Expect(loc).To(Equal(rome))
			loc, err = forge.JobTimeZone(nil, time.UTC)
			Expect(err).ToNot(HaveOccurred())
			Expect(loc).To(Equal(time.UTC))
			_, err = forge.JobTimeZone(&clv1alpha1.JobPolicy{TimeZone: "Europe/Atlantis"}, time.UTC)
			Expect(err).To(HaveOccurred())
			_, err = forge.JobTimeZone(&clv1alpha1.JobPolicy{TimeZone: "Local"}, time.UTC)
			Expect(err).To(HaveOccurred())
		})
	})

	DescribeTable("The forge.JobWindowMinute function",
		func(value string, minute int, valid bool) {
			got, err := forge.JobWindowMinute(value)
			if !valid {
				Expect(err).To(HaveOccurred())
				return
			}
			Expect(err).ToNot(HaveOccurred())
			Expect(got).To(Equal(minute))
		},
		Entry("midnight", "00:00", 0, true),
		Entry("the last minute", "23:59", 1439, true),
		Entry("the end of the day", "24:00", 1440, true),
		Entry("after the end of the day", "24:01", 0, false),
		Entry("60 minutes", "10:60", 0, false),
		Entry("no colon", "1000", 0, false),
	)

	Describe("The forge.JobWindowCovers function", func() {
		evening := window("20:00", "24:00", mon)
		DescribeTable("[start, end) on its days, on the wall clock of the zone",
			func(t time.Time, covered bool) { Expect(forge.JobWindowCovers(&evening, rome, t)).To(Equal(covered)) },
			// 2026-11-02 is a Monday.
			Entry("one second before the start", at(rome, "2026-11-02 19:59:59"), false),
			Entry("at the start", at(rome, "2026-11-02 20:00:00"), true),
			Entry("the last second of a 24:00 end", at(rome, "2026-11-02 23:59:59"), true),
			Entry("at a 24:00 end (Tuesday)", at(rome, "2026-11-03 00:00:00"), false),
			Entry("another day", at(rome, "2026-11-03 21:00:00"), false),
			Entry("the same instant written in UTC (20:30 in Rome)", utc("2026-11-02 19:30:00"), true),
			Entry("20:30 UTC is 21:30 in Rome", utc("2026-11-02 20:30:00"), true),
			Entry("22:59 UTC is 23:59 in Rome", utc("2026-11-02 22:59:00"), true),
			Entry("23:00 UTC is Tuesday in Rome", utc("2026-11-02 23:00:00"), false),
		)
	})

	Describe("The forge.JobLimitsAt function", func() {
		var spec clv1alpha1.WorkspaceSpec
		BeforeEach(func() {
			spec = clv1alpha1.WorkspaceSpec{
				QueueLimits: &clv1alpha1.QueueLimits{
					MaxAtOnce:  quantities("cpu", "4", "nvidia.com/gpu", "1"),
					Guaranteed: quantities("cpu", "1"),
					MaxRunTime: "2h",
				},
				Windows: []clv1alpha1.JobWindow{
					{Days: []clv1alpha1.WeekDay{mon}, Start: "20:00", End: "24:00",
						Overrides: clv1alpha1.QueueLimits{MaxAtOnce: quantities("nvidia.com/gpu", "4"), MaxRunTime: "8h"}},
					{Days: []clv1alpha1.WeekDay{mon, tue}, Start: "18:00", End: "22:00",
						Overrides: clv1alpha1.QueueLimits{Guaranteed: quantities("cpu", "3")}},
				},
			}
		})

		It("Should return the defaults outside every window", func() {
			Expect(forge.JobLimitsAt(&spec, rome, at(rome, "2026-11-02 12:00:00"))).To(Equal(*spec.QueueLimits))
		})

		It("Should replace each field set in the overrides as a whole, and keep the others", func() {
			limits := forge.JobLimitsAt(&spec, rome, at(rome, "2026-11-02 23:00:00"))
			Expect(limits.MaxAtOnce).To(Equal(quantities("nvidia.com/gpu", "4")))
			Expect(limits.MaxRunTime).To(Equal("8h"))
			Expect(limits.Guaranteed).To(Equal(quantities("cpu", "1")))
		})

		It("Should apply the first window in list order that covers the time, never merging two windows", func() {
			limits := forge.JobLimitsAt(&spec, rome, at(rome, "2026-11-02 21:00:00"))
			Expect(limits.MaxAtOnce).To(Equal(quantities("nvidia.com/gpu", "4")))
			Expect(limits.Guaranteed).To(Equal(quantities("cpu", "1")))
			limits = forge.JobLimitsAt(&spec, rome, at(rome, "2026-11-02 19:00:00"))
			Expect(limits.Guaranteed).To(Equal(quantities("cpu", "3")))
			Expect(limits.MaxAtOnce).To(Equal(quantities("cpu", "4", "nvidia.com/gpu", "1")))
		})

		It("Should ignore the windows when the time zone is not valid (nil)", func() {
			Expect(forge.JobLimitsAt(&spec, nil, at(rome, "2026-11-02 21:00:00"))).To(Equal(*spec.QueueLimits))
		})

		It("Should not change the spec of the Workspace", func() {
			limits := forge.JobLimitsAt(&spec, rome, at(rome, "2026-11-02 21:00:00"))
			limits.MaxAtOnce["cpu"] = resource.MustParse("99")
			Expect(spec.Windows[0].Overrides.MaxAtOnce).ToNot(HaveKey("cpu"))
		})

		It("Should list the defaults and each window, in JobLimitsAll", func() {
			Expect(forge.JobLimitsAll(&spec, rome)).To(HaveLen(3))
			Expect(forge.JobLimitsAll(&spec, nil)).To(HaveLen(1))
		})
	})

	Describe("The forge.JobLimitBoundaries function", func() {
		It("Should return the starts and ends strictly after now, in order, with a 24:00 end at midnight", func() {
			windows := []clv1alpha1.JobWindow{window("20:00", "24:00", mon), window("08:00", "09:30", tue)}
			b := forge.JobLimitBoundaries(windows, rome, at(rome, "2026-11-02 19:00:00"))
			Expect(instants(b[:4]...)).To(Equal(instants(
				at(rome, "2026-11-02 20:00:00"), at(rome, "2026-11-03 00:00:00"),
				at(rome, "2026-11-03 08:00:00"), at(rome, "2026-11-03 09:30:00"),
			)))
			// Within 8 days (to Tue 2026-11-10 19:00): each start and end twice.
			Expect(b).To(HaveLen(8))
			next, found := forge.JobNextBoundary(&clv1alpha1.WorkspaceSpec{Windows: windows}, rome, at(rome, "2026-11-02 20:00:00"))
			Expect(found).To(BeTrue())
			Expect(next).To(BeTemporally("==", at(rome, "2026-11-03 00:00:00")))
		})

		It("Should return nothing without windows or without a valid time zone", func() {
			Expect(forge.JobLimitBoundaries(nil, rome, time.Now())).To(BeEmpty())
			Expect(forge.JobLimitBoundaries([]clv1alpha1.JobWindow{window("20:00", "24:00", mon)}, nil, time.Now())).To(BeEmpty())
			_, found := forge.JobNextBoundary(&clv1alpha1.WorkspaceSpec{}, rome, time.Now())
			Expect(found).To(BeFalse())
		})

		It("Should start a window that the clock skips (spring forward, Rome) when the clock jumps", func() {
			// 2026-03-29: 02:00 CET -> 03:00 CEST, at 01:00 UTC. 02:30 does not exist.
			b := forge.JobLimitBoundaries([]clv1alpha1.JobWindow{window("02:30", "04:00", sun)}, rome, utc("2026-03-28 12:00:00"))
			Expect(instants(b[:2]...)).To(Equal(instants(utc("2026-03-29 01:00:00"), utc("2026-03-29 02:00:00"))))
		})

		It("Should start a window that the clock skips (spring forward, New York) when the clock jumps", func() {
			// 2026-03-08: 02:00 EST -> 03:00 EDT, at 07:00 UTC. Go's time.Date picks the instant before the jump here.
			b := forge.JobLimitBoundaries([]clv1alpha1.JobWindow{window("02:30", "04:00", sun)}, newYork, utc("2026-03-07 12:00:00"))
			Expect(instants(b[:2]...)).To(Equal(instants(utc("2026-03-08 07:00:00"), utc("2026-03-08 08:00:00"))))
		})

		It("Should cross a time of the repeated hour twice, and the jump back once (fall back, Rome)", func() {
			// 2026-10-25: 03:00 CEST -> 02:00 CET, at 01:00 UTC. 02:30 happens at 00:30 and at 01:30 UTC.
			b := forge.JobLimitBoundaries([]clv1alpha1.JobWindow{window("02:30", "04:00", sun)}, rome, utc("2026-10-24 12:00:00"))
			Expect(instants(b[:4]...)).To(Equal(instants(
				utc("2026-10-25 00:30:00"), utc("2026-10-25 01:00:00"), utc("2026-10-25 01:30:00"), utc("2026-10-25 03:00:00"),
			)))
		})

		DescribeTable("The coverage of a window changes only at a boundary, minute by minute across a daylight saving day",
			func(loc *time.Location, from string, w clv1alpha1.JobWindow) {
				start := utc(from)
				boundaries := forge.JobLimitBoundaries([]clv1alpha1.JobWindow{w}, loc, start)
				isBoundary := map[int64]bool{}
				for _, b := range boundaries {
					isBoundary[b.Unix()] = true
				}
				changes := 0
				for t := start.Add(time.Minute); t.Before(start.Add(36 * time.Hour)); t = t.Add(time.Minute) {
					if forge.JobWindowCovers(&w, loc, t) != forge.JobWindowCovers(&w, loc, t.Add(-time.Minute)) {
						changes++
						Expect(isBoundary[t.Unix()]).To(BeTrue(), "the coverage changes at %s, which is not a boundary", t.UTC())
					}
				}
				Expect(changes).To(BeNumerically(">=", 2))
			},
			Entry("Rome, spring forward, a window starting in the skipped hour", rome, "2026-03-28 12:00:00", window("02:30", "04:00", sun)),
			Entry("Rome, spring forward, a window ending in the skipped hour", rome, "2026-03-28 12:00:00", window("01:00", "02:30", sun)),
			Entry("Rome, fall back, a window starting in the repeated hour", rome, "2026-10-24 12:00:00", window("02:30", "04:00", sun)),
			Entry("Rome, fall back, a window ending in the repeated hour", rome, "2026-10-24 12:00:00", window("00:00", "02:30", sun)),
			Entry("Rome, fall back, a night window", rome, "2026-10-24 12:00:00", window("00:00", "06:00", sun)),
			Entry("New York, spring forward", newYork, "2026-03-07 12:00:00", window("02:30", "04:00", sun)),
			Entry("New York, fall back", newYork, "2026-10-31 12:00:00", window("01:30", "24:00", sun)),
		)
	})

	Describe("The forge.JobNextTimeAllowing function", func() {
		spec := clv1alpha1.WorkspaceSpec{
			QueueLimits: &clv1alpha1.QueueLimits{MaxRunTime: "2h"},
			Windows: []clv1alpha1.JobWindow{
				{Days: []clv1alpha1.WeekDay{mon}, Start: "18:00", End: "21:00", Overrides: clv1alpha1.QueueLimits{MaxRunTime: "3h"}},
				{Days: []clv1alpha1.WeekDay{mon}, Start: "20:00", End: "24:00", Overrides: clv1alpha1.QueueLimits{MaxRunTime: "8h"}},
			},
		}
		allows6h := func(limits *clv1alpha1.QueueLimits) bool { return limits.MaxRunTime == "8h" }

		It("Should return the first boundary at which the limits in force allow, skipping a window shadowed by an earlier one", func() {
			// From 20:00 to 21:00 the first window applies (3h): the 8h window is in force only from 21:00.
			t, found := forge.JobNextTimeAllowing(&spec, rome, at(rome, "2026-11-02 12:00:00"), allows6h)
			Expect(found).To(BeTrue())
			Expect(t).To(BeTemporally("==", at(rome, "2026-11-02 21:00:00")))
			Expect(forge.FormatJobWindowTime(t, rome)).To(Equal("Mon 21:00"))
		})

		It("Should return false when no window allows, or when the time zone is not valid", func() {
			_, found := forge.JobNextTimeAllowing(&spec, rome, at(rome, "2026-11-02 12:00:00"),
				func(*clv1alpha1.QueueLimits) bool { return false })
			Expect(found).To(BeFalse())
			_, found = forge.JobNextTimeAllowing(&spec, nil, at(rome, "2026-11-02 12:00:00"), allows6h)
			Expect(found).To(BeFalse())
		})
	})
})
