//go:build load

package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
)

const microsPerUnit = 1_000_000

type sample struct {
	name   string
	labels map[string]string
	micros int64
}

type series map[string]sample

func seriesKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	parts := make([]string, 0, len(labels))
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		parts = append(parts, k+"="+strconv.Quote(labels[k]))
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

func parseExposition(r io.Reader, keep func(name string) bool) (series, error) {
	out := series{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		s, ok, err := parseLine(line)
		if err != nil {
			return nil, err
		}
		if !ok || !keep(s.name) {
			continue
		}
		out[seriesKey(s.name, s.labels)] = s
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read exposition: %w", err)
	}
	return out, nil
}

func parseLine(line string) (sample, bool, error) {
	end := strings.IndexAny(line, "{ \t")
	if end <= 0 {
		return sample{}, false, fmt.Errorf("metric line %q has no value", line)
	}
	s := sample{name: line[:end], labels: map[string]string{}}
	rest := line[end:]
	if rest[0] == '{' {
		labels, after, err := parseLabels(rest[1:])
		if err != nil {
			return sample{}, false, fmt.Errorf("metric line %q: %w", line, err)
		}
		s.labels, rest = labels, after
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return sample{}, false, fmt.Errorf("metric line %q has no value", line)
	}
	v, ok := parseMicros(fields[0])
	if !ok {
		return sample{}, false, nil
	}
	s.micros = v
	return s, true, nil
}

func parseLabels(s string) (map[string]string, string, error) {
	labels := map[string]string{}
	for {
		s = strings.TrimLeft(s, " ,")
		if strings.HasPrefix(s, "}") {
			return labels, s[1:], nil
		}
		name, rest, ok := strings.Cut(s, "=")
		if !ok || !strings.HasPrefix(rest, `"`) {
			return nil, "", errors.New("malformed label set")
		}
		value, after, err := unquoteLabel(rest[1:])
		if err != nil {
			return nil, "", err
		}
		labels[strings.TrimSpace(name)] = value
		s = after
	}
}

func unquoteLabel(s string) (string, string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '"':
			return b.String(), s[i+1:], nil
		case '\\':
			if i+1 == len(s) {
				return "", "", errors.New("unterminated escape in label value")
			}
			i++
			switch s[i] {
			case 'n':
				b.WriteByte('\n')
			default:
				b.WriteByte(s[i])
			}
		default:
			b.WriteByte(c)
		}
	}
	return "", "", errors.New("unterminated label value")
}

func parseMicros(s string) (int64, bool) {
	neg := false
	switch {
	case strings.HasPrefix(s, "-"):
		neg, s = true, s[1:]
	case strings.HasPrefix(s, "+"):
		s = s[1:]
	}
	mantissa, exp := s, 0
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.Atoi(s[i+1:])
		if err != nil {
			return 0, false
		}
		mantissa, exp = s[:i], e
	}
	whole, frac, _ := strings.Cut(mantissa, ".")
	if whole+frac == "" || !allDigits(whole+frac) {
		return 0, false
	}
	digits := strings.TrimLeft(whole+frac, "0")
	exp += 6 - len(frac)
	for len(digits) > 18 {
		digits, exp = digits[:len(digits)-1], exp+1
	}
	var v int64
	if digits != "" {
		parsed, err := strconv.ParseInt(digits, 10, 64)
		if err != nil {
			return 0, false
		}
		v = parsed
	}
	for ; exp > 0 && v != 0; exp-- {
		if v > math.MaxInt64/10 {
			return 0, false
		}
		v *= 10
	}
	for ; exp < 0 && v != 0; exp++ {
		v /= 10
	}
	if neg {
		v = -v
	}
	return v, true
}

func counterDelta(before, after series) series {
	out := series{}
	for k, a := range after {
		d := a.micros
		if b, ok := before[k]; ok && a.micros >= b.micros {
			d = a.micros - b.micros
		}
		out[k] = sample{name: a.name, labels: a.labels, micros: d}
	}
	return out
}

func merge(all ...series) series {
	out := series{}
	for _, s := range all {
		for k, v := range s {
			acc := out[k]
			acc.name, acc.labels = v.name, v.labels
			acc.micros += v.micros
			out[k] = acc
		}
	}
	return out
}

func (s series) sum(name string, match map[string]string) int64 {
	var total int64
	for _, v := range s {
		if v.name == name && labelsMatch(v.labels, match) {
			total += v.micros
		}
	}
	return total
}

func (s series) count(name string, match map[string]string) int64 {
	return s.sum(name, match) / microsPerUnit
}

func (s series) byLabel(name, label string) map[string]int64 {
	out := map[string]int64{}
	for _, v := range s {
		if v.name == name {
			out[v.labels[label]] += v.micros
		}
	}
	return out
}

func labelsMatch(labels, match map[string]string) bool {
	for k, want := range match {
		if labels[k] != want {
			return false
		}
	}
	return true
}
