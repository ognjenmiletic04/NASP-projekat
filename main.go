package main

import (
	"bufio"
	"fmt"
	"os"
	"project/memtable"

	//"project/sstable"
	"strings"
)

var manager *Manager

func main() {
	fmt.Println("=== NASP PROJEKAT - LSM TREE SISTEM ===")

	// Izbor tipa memtable-a
	memTableType := chooseMemTableType()

	// Kreiranje Manager-a
	fmt.Printf("\nKreiranje sistema sa %s memtable...\n", memTableType.String())
	manager = NewManager(1024, 1024*5, 5, memTableType)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		showMainMenu()
		fmt.Print("Izbor: ")

		if !scanner.Scan() {
			break
		}

		choice := strings.TrimSpace(scanner.Text())

		switch choice {
		case "1":
			handlePUT(scanner)
		case "2":
			handleGET(scanner)
		case "3":
			handleDELETE(scanner)
		case "4":
			showMemTableContent()
		case "0":
			fmt.Println("Izlazim iz programa...")
			return
		default:
			fmt.Println("Nevaljan izbor! Pokušajte ponovo.")
		}

		fmt.Println()
	}
}

func chooseMemTableType() memtable.MemTableType {
	fmt.Println("\nIzaberite tip MemTable-a:")
	fmt.Println("1. SkipList MemTable")
	fmt.Println("2. HashMap MemTable")
	fmt.Println("3. BTree MemTable")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Izbor (1-3): ")
		if !scanner.Scan() {
			continue
		}

		choice := strings.TrimSpace(scanner.Text())
		switch choice {
		case "1":
			return memtable.TypeSkipList
		case "2":
			return memtable.TypeHashMap
		case "3":
			return memtable.TypeBTree
		default:
			fmt.Println("Nevaljan izbor! Molimo unesite 1, 2 ili 3.")
		}
	}
}

func showMainMenu() {
	fmt.Println("=== GLAVNI MENI ===")
	fmt.Println("1. PUT - Dodaj podatak")
	fmt.Println("2. GET - Pronađi podatak")
	fmt.Println("3. DELETE - Obriši podatak")
	fmt.Println("4. FLUSH - Prikaži sadržaj memtable")
	fmt.Println("0. IZLAZ")
	fmt.Println("-------------------")
}

func handlePUT(scanner *bufio.Scanner) {
	fmt.Print("Unesite ključ: ")
	if !scanner.Scan() {
		return
	}
	key := strings.TrimSpace(scanner.Text())

	fmt.Print("Unesite vrednost: ")
	if !scanner.Scan() {
		return
	}
	value := []byte(strings.TrimSpace(scanner.Text()))

	err := manager.PUT(key, value)
	if err != nil {
		fmt.Printf("GREŠKA: %v\n", err)
	} else {
		fmt.Printf("PUT uspešan: %s = %s\n", key, string(value))
	}
}

func handleGET(scanner *bufio.Scanner) {
	fmt.Print("Unesite ključ: ")
	if !scanner.Scan() {
		return
	}
	key := strings.TrimSpace(scanner.Text())

	result := manager.GET(key)
	if result == nil {
		fmt.Printf("Ključ '%s' nije pronađen\n", key)
	} else {
		fmt.Printf("GET uspešan: %s = %s\n", key, string(result))
	}
}

func handleDELETE(scanner *bufio.Scanner) {
	fmt.Print("Unesite ključ za brisanje: ")
	if !scanner.Scan() {
		return
	}
	key := strings.TrimSpace(scanner.Text())

	err := manager.DELETE(key)
	if err != nil {
		fmt.Printf("GREŠKA: %v\n", err)
	} else {
		fmt.Printf("DELETE uspešan: ključ '%s' je obrisan\n", key)
	}
}

func showMemTableContent() {
	fmt.Println("=== SADRŽAJ MEMTABLE ===")
	size := manager.memtable.GetSize()
	isFull := manager.memtable.IsFull()

	fmt.Printf("Broj zapisa: %d\n", size)
	if isFull {
		fmt.Println("Status: PUN (potreban flush)")
	} else {
		fmt.Println("Status: Ima mesta")
	}
	fmt.Println()

	manager.memtable.Dump()
	fmt.Println("=======================")

}
