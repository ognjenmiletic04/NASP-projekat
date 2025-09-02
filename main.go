package main

import (
	"fmt"
	"log"
)

func main() {
	fmt.Println("=== TESTIRANJE OPTIMIZOVANIH PUT/DELETE/GET METODA ===")
	
	// Kreiraj Manager
	fmt.Println("\n1. Kreiranje Manager-a...")
	manager := NewManager(1024, 1024*5, 5)
	
	// Test scenario: PUT → GET → DELETE → GET
	testKey := "test_kljuc"
	testValue := []byte("test_vrednost")
	
	// KORAK 1: PUT operacija
	fmt.Println("\n2. PUT operacija...")
	fmt.Printf("Dodajem: %s = %s\n", testKey, string(testValue))
	err := manager.PUT(testKey, testValue)
	if err != nil {
		log.Printf("PUT neuspešan: %v", err)
		return
	}
	
	// KORAK 2: GET nakon PUT-a
	fmt.Println("\n3. GET nakon PUT-a...")
	result := manager.GET(testKey)
	if result != nil {
		fmt.Printf("✅ GET uspešan: %s = %s\n", testKey, string(result))
	} else {
		fmt.Printf("❌ GET neuspešan: ključ nije pronađen\n")
	}
	
	// KORAK 3: DELETE operacija
	fmt.Println("\n4. DELETE operacija...")
	fmt.Printf("Brišem ključ: %s\n", testKey)
	err = manager.DELETE(testKey)
	if err != nil {
		log.Printf("DELETE neuspešan: %v", err)
		return
	}
	
	// KORAK 4: GET nakon DELETE-a
	fmt.Println("\n5. GET nakon DELETE-a...")
	result = manager.GET(testKey)
	if result == nil {
		fmt.Printf("✅ GET nakon DELETE-a: ključ je uspešno obrisan\n")
	} else {
		fmt.Printf("❌ Greška: ključ još uvek postoji: %s\n", string(result))
	}
	
	// KORAK 5: Dodaj novi ključ da vidimo da li PUT još uvek radi
	fmt.Println("\n6. PUT novog ključa nakon DELETE-a...")
	newKey := "novi_kljuc"
	newValue := []byte("nova_vrednost")
	fmt.Printf("Dodajem: %s = %s\n", newKey, string(newValue))
	err = manager.PUT(newKey, newValue)
	if err != nil {
		log.Printf("PUT novog ključa neuspešan: %v", err)
		return
	}
	
	// KORAK 6: GET novog ključa
	fmt.Println("\n7. GET novog ključa...")
	result = manager.GET(newKey)
	if result != nil {
		fmt.Printf("✅ GET novog ključa uspešan: %s = %s\n", newKey, string(result))
	} else {
		fmt.Printf("❌ GET novog ključa neuspešan\n")
	}
	
	// KORAK 7: Pokušaj GET obrisanog ključa ponovo
	fmt.Println("\n8. Finalna provala GET obrisanog ključa...")
	result = manager.GET(testKey)
	if result == nil {
		fmt.Printf("✅ Potvrđeno: obrisani ključ '%s' ne postoji\n", testKey)
	} else {
		fmt.Printf("❌ Greška: obrisani ključ još uvek vraća: %s\n", string(result))
	}
	
	fmt.Println("\n=== KRATAK PREGLED MEMTABLE-A ===")
	manager.memtable.Flush()
	
	fmt.Println("\n=== TEST ZAVRŠEN ===")
}

// // TEST SCENARIO - BTree sa Record
// fmt.Println("\n=== TESTING BTREE WITH RECORD ===")

// // Kreiraj BTree
// btree := memtable.NewBTree(3) // min degree = 3

// // Kreiraj test record-e za BTree
// btreeRecord1 := blockmanager.SetRec(0, 1, 0, 4, 6, "key1", []byte("value1"))
// btreeRecord2 := blockmanager.SetRec(0, 2, 0, 4, 6, "key2", []byte("value2"))
// btreeRecord3 := blockmanager.SetRec(0, 3, 0, 4, 6, "key3", []byte("value3"))
// btreeRecord4 := blockmanager.SetRec(0, 4, 0, 4, 6, "key4", []byte("value4"))

// fmt.Printf("Kreiran btreeRecord1: Key=%s, Value=%s\n", btreeRecord1.GetKey(), string(btreeRecord1.GetValue()))
// fmt.Printf("Kreiran btreeRecord2: Key=%s, Value=%s\n", btreeRecord2.GetKey(), string(btreeRecord2.GetValue()))
// fmt.Printf("Kreiran btreeRecord3: Key=%s, Value=%s\n", btreeRecord3.GetKey(), string(btreeRecord3.GetValue()))
// fmt.Printf("Kreiran btreeRecord4: Key=%s, Value=%s\n", btreeRecord4.GetKey(), string(btreeRecord4.GetValue()))
// // Dodaj još test record-a za BTree
// btreeRecord5 := blockmanager.SetRec(0, 5, 0, 4, 6, "key5", []byte("value5"))
// btreeRecord6 := blockmanager.SetRec(0, 6, 0, 4, 6, "key6", []byte("value6"))
// btreeRecord7 := blockmanager.SetRec(0, 7, 0, 4, 6, "key7", []byte("value7"))

// fmt.Printf("Kreiran btreeRecord5: Key=%s, Value=%s\n", btreeRecord5.GetKey(), string(btreeRecord5.GetValue()))
// fmt.Printf("Kreiran btreeRecord6: Key=%s, Value=%s\n", btreeRecord6.GetKey(), string(btreeRecord6.GetValue()))
// fmt.Printf("Kreiran btreeRecord7: Key=%s, Value=%s\n", btreeRecord7.GetKey(), string(btreeRecord7.GetValue()))

// // Dodaj record-e u BTree
// fmt.Println("\nDodajem record-e u BTree...")
// btree.Insert(btreeRecord1)
// btree.Insert(btreeRecord2)
// btree.Insert(btreeRecord3)
// btree.Insert(btreeRecord4)
// btree.Insert(btreeRecord5)
// btree.Insert(btreeRecord6)
// btree.Insert(btreeRecord7)

// fmt.Println("\n Ispis BTree")
// btree.PrintTree()
// // Test search
// fmt.Println("\nTesting BTree search:")
// foundRecord := btree.Search("key2")
// if foundRecord != nil {
// 	fmt.Printf("Found: Key=%s, Value=%s\n", foundRecord.GetKey(), string(foundRecord.GetValue()))
// } else {
// 	fmt.Println("Record not found")
// }

// fmt.Println("\n=== BTREE TEST ZAVRŠEN ===")

// manager := NewManager(1024, 1024*5, 5)
// reader := bufio.NewReader(os.Stdin)
// fmt.Printf("1. PUT\n")
// fmt.Printf("2. GET\n")
// fmt.Printf("3. DELETE\n")
// fmt.Printf("X. EXIT\n")
// fmt.Printf("\n")
// exit := false
// for !exit {
// 	fmt.Print("Enter: ")
// 	choice, _ := reader.ReadString('\n')
// 	choice = strings.TrimSpace(choice)
// 	if len(choice) == 0 {
// 		continue
// 	}
// 	switch choice {
// 	case "1":
// 		fmt.Println("Usage: PUT <key> <value> OR PUT <key> --file <path>")
// 		input, _ := reader.ReadString('\n')
// 		input = strings.TrimSpace(input)
// 		parts := strings.Split(input, " ")
// 		if len(parts) < 3 || len(parts) > 4 {
// 			fmt.Println("Invalid input")
// 			continue
// 		}
// 		key := parts[1]
// 		var value []byte
// 		var err error
// 		if parts[2] == "--file" {
// 			if len(parts) < 4 {
// 				fmt.Println("Invalid input")
// 				continue
// 			}
// 			value, err = os.ReadFile(parts[3])
// 			if err != nil {
// 				if errors.Is(err, os.ErrNotExist) {
// 					fmt.Println("File does not exist")
// 					continue
// 				}
// 			}

// 		} else {
// 			if len(parts) > 3 {
// 				fmt.Println("Invalid input")
// 				continue
// 			}
// 			value = []byte(parts[2])
// 		}
// 		manager.PUT(key, value)

// 	case "2":
// 		fmt.Println("Usage: GET <key>")
// 		input, _ := reader.ReadString('\n')
// 		input = strings.TrimSpace(input)
// 		parts := strings.Split(input, " ")
// 		if len(parts) != 2 {
// 			fmt.Println("Invalid input")
// 			continue
// 		}
// 		manager.GET(parts[1])
// 	case "3":
// 		fmt.Println("Usage: DELETE <key>")
// 		input, _ := reader.ReadString('\n')
// 		input = strings.TrimSpace(input)
// 		parts := strings.Split(input, " ")
// 		if len(parts) != 2 {
// 			fmt.Println("Invalid input")
// 			continue
// 		}
// 		manager.DELETE(parts[1])
// 	case "X":
// 		exit = true
// 	default:
// 		continue
// 	}

// }

// for {
// 	rec := manager.wal.NextRecord(manager.wal.blockManager)
// 	if rec == nil {
// 		break
// 	}

// }
