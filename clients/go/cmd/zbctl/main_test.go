package main

import (
	"context"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/zeebe-io/zeebe/clients/go/internal/containerSuite"
	"io/ioutil"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

var zbctl string

type integrationTestSuite struct {
	*containerSuite.ContainerSuite
}

func TestZbctl(t *testing.T) {
	err := buildZbctl()
	if err != nil {
		t.Fatal(errors.Wrap(err, "couldn't build zbctl"))
	}

	suite.Run(t,
		&integrationTestSuite{
			ContainerSuite: &containerSuite.ContainerSuite{
				Timeout:        time.Second,
				ContainerImage: "camunda/zeebe:current-test",
			},
		})
}

func (s *integrationTestSuite) TestTable() {
	tests := []struct {
		name       string
		command    string
		args       []string
		goldenFile string
		timeout    time.Duration
	}{
		{
			name:       "print help menu",
			command:    "",
			args:       []string{},
			goldenFile: "testdata/help.golden",
			timeout:    2 * time.Second,
		},
	}

	for _, test := range tests {
		s.T().Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), test.timeout)
			defer cancel()

			cmd := exec.CommandContext(ctx, fmt.Sprintf("./dist/%s", zbctl), test.args...)

			cmdOutput, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatal(err)
			}
			got := string(cmdOutput)

			goldenFile, err := ioutil.ReadFile(test.goldenFile)
			if err != nil {
				t.Fatal(err)
			}
			want := string(goldenFile)

			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("%s (-want +got) %s", test.name, diff)
			}
		})
	}
}

func buildZbctl() error {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	if runtime.GOOS == "linux" {
		zbctl = "zbctl"
	} else if runtime.GOOS == "windows" {
		zbctl = "zbctl.exe"
	} else if runtime.GOOS == "darwin" {
		zbctl = "zbctl.darwin"
	} else {
		return errors.Errorf("Can't run zbctl tests on unsupported OS '%s'", runtime.GOOS)
	}

	return exec.CommandContext(ctx, "./build.sh", runtime.GOOS).Run()
}
