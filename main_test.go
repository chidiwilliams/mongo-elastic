package main_test

import (
	"log"
	"os"
	"os/exec"
	"testing"
)

const binaryFilePath = "mongo-elastic"

func TestMain(m *testing.M) {
	cmd := exec.Command("go", "build", binaryFilePath)
	if err := cmd.Run(); err != nil {
		log.Println(err)
		os.Exit(1)
	}

	exitCode := m.Run()
	defer os.Exit(exitCode)

	cleanup()
}

func TestRun(t *testing.T) {
	runBinary(t)
	defer cleanupTest()
}

func runBinary(t testing.TB) {
	cmd := exec.Command("./" + binaryFilePath)
	fatalIfErr(t, cmd.Run())
}

func cleanupTest() {
}

func cleanup() {
}

func fatalIfErr(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
