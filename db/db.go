package db

import (
	"bufio"
	"os"

	"github.com/tidwall/buntdb"
)

type MosmixDB struct {
	db *buntdb.DB
}

func NewMosmixDB(filename string) (*MosmixDB, error) {
	db, err := buntdb.Open(filename)
	if err != nil {
		return &MosmixDB{}, err
	}

	m := &MosmixDB{db: db}
	err = m.createIndexes()
	if err != nil {
		return &MosmixDB{}, err
	}

	return m, nil
}

func (m *MosmixDB) Close() error {
	err := m.db.Shrink()
	if err != nil {
		return err
	}
	return m.db.Close()
}

func (m *MosmixDB) PersistToDisk(filename string) error {
	err := m.db.Shrink()
	if err != nil {
		return err
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	err = m.db.Save(w)
	if err != nil {
		return err
	}
	w.Flush()

	return nil
}
