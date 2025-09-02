package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

func main() {
	// // TEST SCENARIO - memtable sa Record
	// fmt.Println("=== TESTING MEMTABLE WITH RECORD ===")

	// // Kreiraj memtable
	// mt := memtable.NewMemTable()

	// // Kreiraj test record-e
	// record1 := blockmanager.SetRec(0, 1, 0, 4, 6, "key1", []byte("value1"))
	// record2 := blockmanager.SetRec(0, 2, 0, 4, 6, "key2", []byte("value2"))
	// record3 := blockmanager.SetRec(0, 3, 0, 4, 6, "key3", []byte("value3"))

	// fmt.Printf("Kreiran record1: Key=%s, Value=%s\n", record1.GetKey(), string(record1.GetValue()))
	// fmt.Printf("Kreiran record2: Key=%s, Value=%s\n", record2.GetKey(), string(record2.GetValue()))
	// fmt.Printf("Kreiran record3: Key=%s, Value=%s\n", record3.GetKey(), string(record3.GetValue()))

	// // Dodaj record-e u memtable
	// fmt.Println("\nDodajem record-e u memtable...")
	// mt.PutNode(record1)
	// mt.PutNode(record2)
	// mt.PutNode(record3)

	// // Flush memtable da vidimo sadržaj
	// fmt.Println("\nFlush memtable:")
	// mt.Flush()

	// fmt.Println("\n=== TEST ZAVRŠEN ===")

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
