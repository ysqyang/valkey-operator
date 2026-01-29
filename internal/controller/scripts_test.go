package controller

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestLivenessCheckScript(t *testing.T) {
	scriptPath := filepath.Join("scripts", "liveness-check.sh")

	tests := []struct {
		name     string
		response string
		wantErr  bool
	}{
		{name: "pong", response: "PONG", wantErr: false},
		{name: "loading", response: "LOADING 123", wantErr: false},
		{name: "masterdown", response: "MASTERDOWN Link with MASTER is down", wantErr: false},
		{name: "error", response: "ERR something bad", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := runProbeScript(t, scriptPath, test.response); (err != nil) != test.wantErr {
				t.Fatalf("unexpected result: err=%v wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestReadinessCheckScript(t *testing.T) {
	scriptPath := filepath.Join("scripts", "readiness-check.sh")

	tests := []struct {
		name     string
		response string
		wantErr  bool
	}{
		{name: "pong", response: "PONG", wantErr: false},
		{name: "loading", response: "LOADING 123", wantErr: true},
		{name: "masterdown", response: "MASTERDOWN Link with MASTER is down", wantErr: true},
		{name: "error", response: "ERR something bad", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := runProbeScript(t, scriptPath, test.response); (err != nil) != test.wantErr {
				t.Fatalf("unexpected result: err=%v wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func runProbeScript(t *testing.T, scriptPath, response string) error {
	t.Helper()

	binDir := t.TempDir()
	valkeyCli := filepath.Join(binDir, "valkey-cli")
	script := []byte("#!/bin/sh\n" +
		"echo \"${VALKEY_RESPONSE:-PONG}\"\n")
	if err := os.WriteFile(valkeyCli, script, 0o755); err != nil {
		t.Fatalf("write valkey-cli stub: %v", err)
	}

	cmd := exec.Command(scriptPath)
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"VALKEY_RESPONSE="+response,
	)
	return cmd.Run()
}
