package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandaloneStorageReader struct {
	tx *badger.Txn
}

func NewStandaloneStorageReader(tx *badger.Txn) *StandaloneStorageReader {
	return &StandaloneStorageReader{
		tx: tx,
	}
}

func (ssr *StandaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(ssr.tx, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, nil
}

func (ssr *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, ssr.tx)
}

func (ssr *StandaloneStorageReader) Close() {
	ssr.tx.Discard()
}
