package rootstate

import (
	"encoding/json"

	"github.com/blbgo/general"
	"github.com/blbgo/record/root"
)

type rootState struct {
	root.Item
}

// New creates a PersistentState implemented by recordState
func New(theRoot root.Root) (general.PersistentState, error) {
	item, err := theRoot.RootItem(
		"github.com/blbgo/record/rootstate",
		"github.com/blbgo/record/rootstate root item",
	)
	if err != nil {
		return nil, err
	}
	return &rootState{Item: item}, nil
}

// **************** implement PersistentState

func (r *rootState) Save(name string, state interface{}) error {
	value, err := json.Marshal(state)
	if err != nil {
		return err
	}
	key := []byte(name)
	item, err := r.Item.ReadChild(key)
	if err == root.ErrItemNotFound {
		return r.Item.QuickChild(key, value)
	}
	if err != nil {
		return err
	}
	return item.UpdateValue(value)
}

func (r *rootState) Retrieve(name string, state interface{}) error {
	item, err := r.Item.ReadChild([]byte(name))
	if err != nil {
		return err
	}
	return json.Unmarshal(item.Value(), state)
}
