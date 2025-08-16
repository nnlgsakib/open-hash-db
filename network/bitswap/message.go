package bitswap

import (
	"encoding/json"
	"openhashdb/core/block"
	"openhashdb/core/hasher"
)

// Message is the structure of a bitswap message.
type Message struct {
	Wantlist Wantlist        `json:"wantlist"`
	Blocks   []block.Block `json:"blocks"`
}

// Wantlist represents a list of wanted blocks.
type Wantlist struct {
	Entries []WantlistEntry `json:"entries"`
	Full    bool            `json:"full"` // If true, this is the full wantlist
}

// WantlistEntry represents a single entry in a wantlist.
type WantlistEntry struct {
	Hash     hasher.Hash `json:"hash"`
	Priority int         `json:"priority"`
}

// --- Custom JSON Marshaling ---

type jsonMessage struct {
	Wantlist Wantlist    `json:"wantlist"`
	Blocks   []*jsonBlock `json:"blocks"`
}

type jsonBlock struct {
	Hash hasher.Hash `json:"hash"`
	Data []byte      `json:"data"`
}

func (m *Message) MarshalJSON() ([]byte, error) {
	jb := make([]*jsonBlock, len(m.Blocks))
	for i, b := range m.Blocks {
		jb[i] = &jsonBlock{Hash: b.Hash(), Data: b.RawData()}
	}
	return json.Marshal(jsonMessage{
		Wantlist: m.Wantlist,
		Blocks:   jb,
	})
}

func (m *Message) UnmarshalJSON(data []byte) error {
	var jm jsonMessage
	if err := json.Unmarshal(data, &jm); err != nil {
		return err
	}
	m.Wantlist = jm.Wantlist
	m.Blocks = make([]block.Block, len(jm.Blocks))
	for i, jb := range jm.Blocks {
		m.Blocks[i] = block.NewBlockWithHash(jb.Hash, jb.Data)
	}
	return nil
}