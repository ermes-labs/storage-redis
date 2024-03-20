package main_test

import (
	"testing"

	spec "github.com/ermes-labs/spec-tests"
)

func TestSpec(t *testing.T) {
	spec.RunTests(t, &env)
}
