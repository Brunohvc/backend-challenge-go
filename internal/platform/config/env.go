package config

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"time"
)

type env struct {
	lookup   func(string) (string, bool)
	problems []problem
}

func (e *env) value(name string) (string, bool) {
	v, ok := e.lookup(name)
	return v, ok && v != ""
}

func (e *env) add(name, reason string) {
	e.problems = append(e.problems, problem{name: name, reason: reason})
}

func (e *env) invalid(names ...string) bool {
	return slices.ContainsFunc(e.problems, func(p problem) bool {
		return slices.Contains(names, p.name)
	})
}

func (e *env) err() error {
	if len(e.problems) == 0 {
		return nil
	}
	problems := slices.Clone(e.problems)
	slices.SortStableFunc(problems, func(a, b problem) int {
		return cmp.Compare(a.name, b.name)
	})
	return &validationError{problems: problems}
}

func parsed[T any](e *env, name string, def T, parse func(raw string) (T, error)) T {
	raw, ok := e.value(name)
	if !ok {
		return def
	}
	v, err := parse(raw)
	if err != nil {
		e.add(name, err.Error())
		return def
	}
	return v
}

func str(e *env, name, def string) string {
	if v, ok := e.value(name); ok {
		return v
	}
	return def
}

func required(e *env, name string) string {
	v := str(e, name, "")
	if v == "" {
		e.add(name, "is required")
	}
	return v
}

func boolean(e *env, name string, def bool) bool {
	return parsed(e, name, def, func(raw string) (bool, error) {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return false, fmt.Errorf("must be a boolean, got %q", raw)
		}
		return b, nil
	})
}

func duration(e *env, name string, def time.Duration) time.Duration {
	return parsed(e, name, def, func(raw string) (time.Duration, error) {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return 0, fmt.Errorf("must be a duration such as 500ms or 30s, got %q", raw)
		}
		if d <= 0 {
			return 0, fmt.Errorf("must be positive, got %s", d)
		}
		return d, nil
	})
}

type integral interface{ ~int | ~int32 | ~int64 }

func integer[T integral](e *env, name string, def, lo T) T {
	return parsed(e, name, def, func(raw string) (T, error) {
		n, err := parseInt[T](raw)
		if err != nil {
			return 0, err
		}
		if n < lo {
			return 0, fmt.Errorf("must be at least %d, got %d", lo, n)
		}
		return n, nil
	})
}

func integerBetween[T integral](e *env, name string, def, lo, hi T) T {
	return parsed(e, name, def, func(raw string) (T, error) {
		n, err := parseInt[T](raw)
		if err != nil {
			return 0, err
		}
		if n < lo || n > hi {
			return 0, fmt.Errorf("must be between %d and %d, got %d", lo, hi, n)
		}
		return n, nil
	})
}

func parseInt[T integral](raw string) (T, error) {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("must be an integer, got %q", raw)
	}
	v := T(n)
	if int64(v) != n {
		return 0, fmt.Errorf("is out of range, got %q", raw)
	}
	return v, nil
}
