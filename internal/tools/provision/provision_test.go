package provision

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestPrincipals_LeastPrivilege(t *testing.T) {
	opt := Options{InputQueue: "in", InputDLQ: "in-dlq", EventsQueue: "events", EventsDLQ: "events-dlq", Providers: []string{"provider-a"}}
	queues := map[string]queue{}
	for _, name := range []string{"in", "in-dlq", "events", "events-dlq"} {
		queues[name] = queue{arn: "arn:" + name}
	}
	consume := []string{"arn:in"}
	want := map[string]map[string][]string{
		"wallet-app": {
			"sqs:ReceiveMessage":               consume,
			"sqs:DeleteMessage":                consume,
			"sqs:ChangeMessageVisibility":      consume,
			"sqs:ChangeMessageVisibilityBatch": consume,
			"sqs:GetQueueAttributes":           {"arn:events", "arn:in", "arn:in-dlq"},
			"sqs:GetQueueUrl":                  {"arn:events", "arn:in", "arn:in-dlq"},
			"sqs:SendMessage":                  {"arn:events", "arn:in-dlq"},
			"sqs:SendMessageBatch":             {"arn:events"},
		},
		"events-reader": {
			"sqs:ReceiveMessage":          {"arn:events"},
			"sqs:DeleteMessage":           {"arn:events"},
			"sqs:ChangeMessageVisibility": {"arn:events"},
			"sqs:GetQueueAttributes":      {"arn:events"},
			"sqs:GetQueueUrl":             {"arn:events"},
		},
		"provider-a-sqs": {
			"sqs:SendMessage": {"arn:in"},
			"sqs:GetQueueUrl": {"arn:in"},
		},
	}

	got := make(map[string]map[string][]string)
	for _, p := range principals(opt, queues) {
		grants := make(map[string][]string)
		for _, st := range p.statements {
			if st.Effect != "Allow" {
				t.Errorf("user %s has a %s statement, want Allow only", p.user, st.Effect)
			}
			for _, action := range st.Action {
				grants[action] = append(grants[action], st.Resource...)
			}
		}
		for action, resources := range grants {
			slices.Sort(resources)
			grants[action] = slices.Compact(resources)
		}
		got[p.user] = grants
	}

	if !maps.EqualFunc(got, want, func(g, w map[string][]string) bool { return maps.EqualFunc(g, w, slices.Equal) }) {
		t.Errorf("grants by user and action =\n%v\nwant\n%v", got, want)
	}
}

func TestStoredCredentials(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string, mode os.FileMode) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), mode); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return path
	}
	valid := write("valid", "[wallet-app]\naws_access_key_id = AKIAWALLET\naws_secret_access_key = secret\n", 0o600)

	tests := []struct {
		name    string
		path    string
		profile string
		want    Credentials
		wantErr bool
	}{
		{"stored profile", valid, "wallet-app", Credentials{"AKIAWALLET", "secret"}, false},
		{"profile not in the file", valid, "provider-a", Credentials{}, false},
		{"no file yet", filepath.Join(dir, "missing"), "wallet-app", Credentials{}, false},
		{"file not readable", write("unreadable", "[wallet-app]\n", 0o000), "wallet-app", Credentials{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := storedCredentials(t.Context(), tt.path, tt.profile)

			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, want error: %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("credentials = %+v, want %+v", got, tt.want)
			}
		})
	}
}
