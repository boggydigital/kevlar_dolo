package kvas_dolo

import (
	"errors"
	"fmt"
	"github.com/boggydigital/dolo"
	"github.com/boggydigital/kvas"
	"io"
)

type IndexSetter struct {
	kv  kvas.KeyValues
	ids []string
}

func NewIndexSetter(kv kvas.KeyValues, ids ...string) dolo.IndexSetter {
	return &IndexSetter{
		kv:  kv,
		ids: ids,
	}
}

func (is *IndexSetter) Len() int {
	return len(is.ids)
}

func (is *IndexSetter) Exists(int) bool {
	//kvas performs hash computation to track modified files,
	//so we want all set attempts to go through (we need to
	//read src to compute that hash)
	return false
}

func (is *IndexSetter) Set(index int, src io.ReadCloser, results chan *dolo.IndexResult, errors chan *dolo.IndexError) {

	defer src.Close()

	if index < 0 || index >= len(is.ids) {
		errors <- dolo.NewIndexError(index, fmt.Errorf("id index out of bounds"))
	}

	if err := is.kv.Set(is.ids[index], src); err != nil {
		errors <- dolo.NewIndexError(index, err)
	}

	results <- dolo.NewIndexResult(index, true)
}

func (is *IndexSetter) Get(index int) (io.ReadCloser, error) {
	if index < 0 || index >= len(is.ids) {
		return nil, errors.New("kvas index out of bounds")
	}
	return is.kv.Get(is.ids[index])
}

func (is *IndexSetter) IsModifiedAfter(index int, since int64) bool {
	if index < 0 || index >= len(is.ids) {
		return false
	}
	return is.kv.IsModifiedAfter(is.ids[index], since)
}
