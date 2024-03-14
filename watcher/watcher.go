package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
)

func main() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	log.Println("Changes to ermeslib.lua will trigger a reload of the Lua script in Redis.\nPress enter to run tests.")

	input := make(chan bool)
	done := make(chan bool)
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Println("Modified file:", event.Name)
				if filepath.Base(event.Name) == "ermeslib.lua" {
					runInitScript()
				}

			case <-input:
				log.Println("Running tests...")
				runTests()

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	// Watch the current directory
	// err = watcher.Add("./packages/go")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// Watch the specific script.lua file located one directory up
	err = watcher.Add("../ermeslib.lua")
	if err != nil {
		log.Fatal(err)
	}

	// Wait for user input
	go func() {
		// Each time user enters a line, run the tests
		for true {
			_, _ = fmt.Scanln()
			input <- true
		}
	}()

	<-done
}

func runTests() {
	cmd := exec.Command("go", "test", "../test/...")

	// Execute the command
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Println("error:", err)
	}

	log.Println("output", string(output))
}

func runInitScript() {
	// Read the content of the Lua script file
	luaScript, err := os.ReadFile("../ermeslib.lua")
	if err != nil {
		panic(err) // Handle error appropriately
	}

	// Create a command for 'redis-cli' and set its standard input
	cmd := exec.Command("redis-cli", "-x", "FUNCTION", "LOAD", "REPLACE")
	cmd.Stdin = bytes.NewReader(luaScript) // Use the Lua script content as stdin

	// Execute the command
	output, err := cmd.CombinedOutput()
	if err != nil {
		panic(err) // Handle error appropriately
	}

	// Print the output from 'redis-cli'
	log.Println("output", string(output))
}
