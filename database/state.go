package database

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Account string

type Tx struct {
	From  Account `json:"from"`
	To    Account `json:"to"`
	Value uint    `json:"value"`
	Data  string  `json:"data"`
}

type State struct {
	Balances  map[Account]uint
	txMempool []Tx
	dbFile    *os.File
}

func (t Tx) IsReward() bool {
	return t.Data == "reward"
}

func NewStateFromDisk() (*State, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	// Get the genesis file from the database folder
	gfp := filepath.Join(cwd, "database", "genesis.json")
	gen, err := loadGenesis(gfp)
	if err != nil {
		return nil, err
	}

	balances := make(map[Account]uint)
	for account, balance := range gen.Balances {
		balances[account] = balance
	}

	// Read the contents of the transaction database file
	txDbFilePath := filepath.Join(cwd, "database", "tx.db")
	f, err := os.OpenFile(txDbFilePath, os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(f)
	state := &State{balances, make([]Tx, 0), f}
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, err
		}

		// Convert JSON encoded TX into an object (struct)
		var tx Tx
		err = json.Unmarshal(scanner.Bytes(), &tx)
		if err != nil {
			return nil, err
		}

		if err := state.apply(tx); err != nil {
			return nil, err
		}
	}

	return state, nil
}

func (s *State) Persist() error {
	mempool := make([]Tx, len(s.txMempool))
	copy(mempool, s.txMempool)
	for i := 0; i < len(mempool); i++ {
		txJson, err := json.Marshal(mempool[i])
		if err != nil {
			return err
		}

		if _, err = s.dbFile.Write(append(txJson, '\n')); err != nil {
			return err
		}
		s.txMempool = s.txMempool[1:]
	}

	return nil
}

func (s *State) Add(tx Tx) error {
	if err := s.apply(tx); err != nil {
		return err
	}

	s.txMempool = append(s.txMempool, tx)
	return nil
}

func (s *State) Close() bool {
	err := s.dbFile.Close()
	if err != nil {
		return false
	}
	return true
}

func (s *State) apply(tx Tx) error {
	if tx.IsReward() {
		s.Balances[tx.To] += tx.Value
		return nil
	}

	if s.Balances[tx.From] < tx.Value {
		return fmt.Errorf("insufficient balance")
	}

	s.Balances[tx.From] -= tx.Value
	s.Balances[tx.To] += tx.Value

	return nil
}
