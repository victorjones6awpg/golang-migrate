package main

import (
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentMigrations(t *testing.T) {
	dbServer := NewMockDBServer()
	numInstances := 10
	var wg sync.WaitGroup
	errorsChan := make(chan error, numInstances)

	for i := 1; i <= numInstances; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			connID := fmt.Sprintf("conn-%d", id)
			driver := NewPostgresDriver(connID, "test_db", dbServer)
			defer driver.Close()

			m, err := NewMigrate(driver)
			if err != nil {
				errorsChan <- fmt.Errorf("instance %d init error: %w", id, err)
				return
			}

			if err := m.Up(); err != nil {
				errorsChan <- fmt.Errorf("instance %d migration error: %w", id, err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errorsChan)

	for err := range errorsChan {
		t.Errorf("Unexpected error during concurrent migrations: %v", err)
	}

	// Verify final state
	dbServer.mu.Lock()
	finalVersion := dbServer.versions["test_db"]
	dirty := dbServer.dirties["test_db"]
	dbServer.mu.Unlock()

	if finalVersion != 3 {
		t.Errorf("Expected final version to be 3, got %d", finalVersion)
	}
	if dirty {
		t.Errorf("Expected database to not be dirty")
	}
}

func TestConnectionResilience(t *testing.T) {
	dbServer := NewMockDBServer()
	
	// 1. Create a connection and acquire lock
	driver1 := NewPostgresDriver("conn-1", "test_db", dbServer)
	if err := driver1.Lock(); err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// 2. Verify lock is held
	dbServer.mu.Lock()
	owner := dbServer.locks["test_db"]
	dbServer.mu.Unlock()
	if owner != "conn-1" {
		t.Fatalf("Expected conn-1 to hold lock, got %s", owner)
	}

	// 3. Simulate connection dying by closing it
	if err := driver1.Close(); err != nil {
		t.Fatalf("Failed to close driver: %v", err)
	}

	// 4. Verify lock is released automatically
	dbServer.mu.Lock()
	owner = dbServer.locks["test_db"]
	dbServer.mu.Unlock()
	if owner != "" {
		t.Fatalf("Expected lock to be released, but it is still held by %s", owner)
	}

	// 5. Verify another connection can now acquire the lock
	driver2 := NewPostgresDriver("conn-2", "test_db", dbServer)
	defer driver2.Close()
	if err := driver2.Lock(); err != nil {
		t.Fatalf("Failed to acquire lock after release: %v", err)
	}

	dbServer.mu.Lock()
	owner = dbServer.locks["test_db"]
	dbServer.mu.Unlock()
	if owner != "conn-2" {
		t.Fatalf("Expected conn-2 to hold lock, got %s", owner)
	}
}
