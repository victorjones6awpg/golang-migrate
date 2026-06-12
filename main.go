package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Driver interface matching golang-migrate's database.Driver
type Driver interface {
	Lock() error
	Unlock() error
	Version() (int, bool, error)
	SetVersion(version int, dirty bool) error
	Close() error
}

// MockDBServer simulates a database server that supports advisory locks
type MockDBServer struct {
	mu       sync.Mutex
	locks    map[string]string // dbName -> connID holding the lock
	conds    map[string]*sync.Cond
	versions map[string]int
	dirties  map[string]bool
}

func NewMockDBServer() *MockDBServer {
	return &MockDBServer{
		locks:    make(map[string]string),
		conds:    make(map[string]*sync.Cond),
		versions: make(map[string]int),
		dirties:  make(map[string]bool),
	}
}

func (s *MockDBServer) AcquireAdvisoryLock(connID, dbName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cond, exists := s.conds[dbName]
	if !exists {
		cond = sync.NewCond(&s.mu)
		s.conds[dbName] = cond
	}

	for {
		owner, locked := s.locks[dbName]
		if !locked {
			s.locks[dbName] = connID
			return nil
		}
		if owner == connID {
			return nil // Already holds the lock
		}
		cond.Wait()
	}
}

func (s *MockDBServer) ReleaseAdvisoryLock(connID, dbName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	owner, locked := s.locks[dbName]
	if !locked {
		return nil
	}
	if owner != connID {
		return fmt.Errorf("connection %s does not hold the lock for %s", connID, dbName)
	}

	delete(s.locks, dbName)
	if cond, exists := s.conds[dbName]; exists {
		cond.Broadcast()
	}
	return nil
}

func (s *MockDBServer) ReleaseAllLocks(connID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for dbName, owner := range s.locks {
		if owner == connID {
			delete(s.locks, dbName)
			if cond, exists := s.conds[dbName]; exists {
				cond.Broadcast()
			}
		}
	}
}

// PostgresDriver implements Driver for PostgreSQL
type PostgresDriver struct {
	connID   string
	dbName   string
	dbServer *MockDBServer
	closed   bool
	mu       sync.Mutex
}

func NewPostgresDriver(connID, dbName string, dbServer *MockDBServer) *PostgresDriver {
	return &PostgresDriver{
		connID:   connID,
		dbName:   dbName,
		dbServer: dbServer,
	}
}

func (p *PostgresDriver) Lock() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("connection closed")
	}
	p.mu.Unlock()
	return p.dbServer.AcquireAdvisoryLock(p.connID, p.dbName)
}

func (p *PostgresDriver) Unlock() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("connection closed")
	}
	p.mu.Unlock()
	return p.dbServer.ReleaseAdvisoryLock(p.connID, p.dbName)
}

func (p *PostgresDriver) Version() (int, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, false, errors.New("connection closed")
	}

	p.dbServer.mu.Lock()
	defer p.dbServer.mu.Unlock()

	if p.dbServer.locks[p.dbName] != p.connID {
		return 0, false, errors.New("database lock must be acquired before reading version")
	}

	return p.dbServer.versions[p.dbName], p.dbServer.dirties[p.dbName], nil
}

func (p *PostgresDriver) SetVersion(version int, dirty bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("connection closed")
	}

	p.dbServer.mu.Lock()
	defer p.dbServer.mu.Unlock()

	if p.dbServer.locks[p.dbName] != p.connID {
		return errors.New("database lock must be acquired before setting version")
	}

	p.dbServer.versions[p.dbName] = version
	p.dbServer.dirties[p.dbName] = dirty
	return nil
}

func (p *PostgresDriver) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	p.dbServer.ReleaseAllLocks(p.connID)
	return nil
}

// MySQLDriver implements Driver for MySQL
type MySQLDriver struct {
	connID   string
	dbName   string
	dbServer *MockDBServer
	closed   bool
	mu       sync.Mutex
}

func NewMySQLDriver(connID, dbName string, dbServer *MockDBServer) *MySQLDriver {
	return &MySQLDriver{
		connID:   connID,
		dbName:   dbName,
		dbServer: dbServer,
	}
}

func (m *MySQLDriver) Lock() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("connection closed")
	}
	m.mu.Unlock()
	return m.dbServer.AcquireAdvisoryLock(m.connID, m.dbName)
}

func (m *MySQLDriver) Unlock() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("connection closed")
	}
	m.mu.Unlock()
	return m.dbServer.ReleaseAdvisoryLock(m.connID, m.dbName)
}

func (m *MySQLDriver) Version() (int, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, false, errors.New("connection closed")
	}

	m.dbServer.mu.Lock()
	defer m.dbServer.mu.Unlock()

	if m.dbServer.locks[m.dbName] != m.connID {
		return 0, false, errors.New("database lock must be acquired before reading version")
	}

	return m.dbServer.versions[m.dbName], m.dbServer.dirties[m.dbName], nil
}

func (m *MySQLDriver) SetVersion(version int, dirty bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("connection closed")
	}

	m.dbServer.mu.Lock()
	defer m.dbServer.mu.Unlock()

	if m.dbServer.locks[m.dbName] != m.connID {
		return errors.New("database lock must be acquired before setting version")
	}

	m.dbServer.versions[m.dbName] = version
	m.dbServer.dirties[m.dbName] = dirty
	return nil
}

func (m *MySQLDriver) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	m.dbServer.ReleaseAllLocks(m.connID)
	return nil
}

// CockroachDriver implements Driver for CockroachDB
type CockroachDriver struct {
	connID   string
	dbName   string
	dbServer *MockDBServer
	closed   bool
	mu       sync.Mutex
}

func NewCockroachDriver(connID, dbName string, dbServer *MockDBServer) *CockroachDriver {
	return &CockroachDriver{
		connID:   connID,
		dbName:   dbName,
		dbServer: dbServer,
	}
}

func (c *CockroachDriver) Lock() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("connection closed")
	}
	c.mu.Unlock()
	return c.dbServer.AcquireAdvisoryLock(c.connID, c.dbName)
}

func (c *CockroachDriver) Unlock() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("connection closed")
	}
	c.mu.Unlock()
	return c.dbServer.ReleaseAdvisoryLock(c.connID, c.dbName)
}

func (c *CockroachDriver) Version() (int, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, false, errors.New("connection closed")
	}

	c.dbServer.mu.Lock()
	defer c.dbServer.mu.Unlock()

	if c.dbServer.locks[c.dbName] != c.connID {
		return 0, false, errors.New("database lock must be acquired before reading version")
	}

	return c.dbServer.versions[c.dbName], c.dbServer.dirties[c.dbName], nil
}

func (c *CockroachDriver) SetVersion(version int, dirty bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("connection closed")
	}

	c.dbServer.mu.Lock()
	defer c.dbServer.mu.Unlock()

	if c.dbServer.locks[c.dbName] != c.connID {
		return errors.New("database lock must be acquired before setting version")
	}

	c.dbServer.versions[c.dbName] = version
	c.dbServer.dirties[c.dbName] = dirty
	return nil
}

func (c *CockroachDriver) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.dbServer.ReleaseAllLocks(c.connID)
	return nil
}

// Migrate orchestrates the migration process
type Migrate struct {
	driver Driver
}

func NewMigrate(driver Driver) (*Migrate, error) {
	return &Migrate{
		driver: driver,
	}, nil
}

func (m *Migrate) Up() error {
	if err := m.driver.Lock(); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer func() {
		_ = m.driver.Unlock()
	}()

	version, dirty, err := m.driver.Version()
	if err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}

	if dirty {
		return errors.New("database is in a dirty state")
	}

	targetVersion := 3
	if version >= targetVersion {
		return nil
	}

	for v := version + 1; v <= targetVersion; v++ {
		time.Sleep(10 * time.Millisecond)
		if err := m.driver.SetVersion(v, false); err != nil {
			return fmt.Errorf("failed to set version: %w", err)
		}
	}

	return nil
}

func (m *Migrate) Down() error {
	if err := m.driver.Lock(); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer func() {
		_ = m.driver.Unlock()
	}()

	version, dirty, err := m.driver.Version()
	if err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}

	if dirty {
		return errors.New("database is in a dirty state")
	}

	if version <= 0 {
		return nil
	}

	for v := version - 1; v >= 0; v-- {
		time.Sleep(10 * time.Millisecond)
		if err := m.driver.SetVersion(v, false); err != nil {
			return fmt.Errorf("failed to set version: %w", err)
		}
	}

	return nil
}

func main() {
	fmt.Println("Running concurrent migration simulation...")
	dbServer := NewMockDBServer()

	var wg sync.WaitGroup
	numInstances := 5

	for i := 1; i <= numInstances; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			connID := fmt.Sprintf("conn-%d", id)
			driver := NewPostgresDriver(connID, "test_db", dbServer)
			defer driver.Close()

			m, err := NewMigrate(driver)
			if err != nil {
				fmt.Printf("Instance %d: failed to initialize: %v\n", id, err)
				return
			}

			fmt.Printf("Instance %d: attempting to run migrations...\n", id)
			if err := m.Up(); err != nil {
				fmt.Printf("Instance %d: migration failed: %v\n", id, err)
			} else {
				fmt.Printf("Instance %d: migration completed successfully\n", id)
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("Simulation finished.")
}
