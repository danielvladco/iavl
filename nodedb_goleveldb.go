//go:build goleveldb

package iavl

import dbm "github.com/tendermint/tm-db"

// Has checks if a hash exists in the database.
func (ndb *nodeDB) Has(hash []byte) (bool, error) {
	key := ndb.nodeKey(hash)

	if ldb, ok := ndb.db.(*dbm.GoLevelDB); ok {
		exists, err := ldb.DB().Has(key, nil)
		if err != nil {
			return false, err
		}
		return exists, nil
	}
	value, err := ndb.db.Get(key)
	if err != nil {
		return false, err
	}

	return value != nil, nil
}
