package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

func main() {
	manager := NewManager(1024, 1024*5, 5)
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("1. PUT\n")
	fmt.Printf("2. GET\n")
	fmt.Printf("3. DELETE\n")
	fmt.Printf("X. EXIT\n")
	fmt.Printf("\n")
	exit := false
	for !exit {
		fmt.Print("Enter: ")
		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)
		if len(choice) == 0 {
			continue
		}
		switch choice {
		case "1":
			fmt.Println("Usage: PUT <key> <value> OR PUT <key> --file <path>")
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			parts := strings.Split(input, " ")
			if len(parts) < 3 || len(parts) > 4 {
				fmt.Println("Invalid input")
				continue
			}
			key := parts[1]
			var value []byte
			var err error
			if parts[2] == "--file" {
				if len(parts) < 4 {
					fmt.Println("Invalid input")
					continue
				}
				value, err = os.ReadFile(parts[3])
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						fmt.Println("File does not exist")
						continue
					}
				}

			} else {
				if len(parts) > 3 {
					fmt.Println("Invalid input")
					continue
				}
				value = []byte(parts[2])
			}
			manager.PUT(key, value)

		case "2":
			fmt.Println("Usage: GET <key>")
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			parts := strings.Split(input, " ")
			if len(parts) != 2 {
				fmt.Println("Invalid input")
				continue
			}
			manager.GET(parts[1])
		case "3":
			fmt.Println("Usage: DELETE <key>")
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			parts := strings.Split(input, " ")
			if len(parts) != 2 {
				fmt.Println("Invalid input")
				continue
			}
			manager.DELETE(parts[1])
		case "X":
			exit = true
		default:
			continue
		}

	}
	// for {
	// 	rec := manager.wal.NextRecord(manager.wal.blockManager)
	// 	if rec == nil {
	// 		break
	// 	}

	// }
}
