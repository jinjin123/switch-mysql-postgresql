package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime/debug"

	"github.com/kr/pretty"
)

func forever(fn func()) {
	f := func() {
		defer func() {
			if r := recover(); r != nil {
				debug.PrintStack()
				pretty.Println("Recover from error:", r)
			}
		}()
		fn()
	}
	for {
		f()
	}
}
func execCommand(commandName string, params []string) bool {
	cmd := exec.Command(commandName, params...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Println(err)
		return false
	}
	cmd.Start()
	//read even line
	reader := bufio.NewReader(stdout)
	for {
		line, err2 := reader.ReadString('\n')
		if err2 != nil || io.EOF == err2 {
			break
		}
		if 0 == len(line) || line == "\n" {
			continue
		}
		fmt.Print(line)
	}

	cmd.Wait()
	return true
}

func main() {
	args := []string{"create_schema", "add_source", "init_replica", "drop_replica", "enable_replica", "start_replica", "disable_replica", "show_status"}
	if (len(os.Args) == 2) && (os.Args[1] == "--init") {
		params := args[1:3]
		execCommand("chameleon.py", []string{"create_schema"})
		for _, b := range params {
			fmt.Print(b)
			execCommand("chameleon.py", []string{b})
		}
	}
	if (len(os.Args) == 1) || (os.Args[1] == "--start") {
		params := args[3:5]
		for _, b := range params {
			fmt.Print(b)
			execCommand("chameleon.py", []string{b})
		}
		forever(func() {
			execCommand("chameleon.py", []string{"start_replica"})
		})
	}
	if (len(os.Args) == 2) && (os.Args[1] == "--status") {
		execCommand("chameleon.py", []string{args[7]})
	}
	if (len(os.Args) == 2) && (os.Args[1] == "--disable") {
		execCommand("chameleon.py", []string{args[6]})
	}
}
