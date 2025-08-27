package rest

import (
    "fmt"
    "net/http"

    "github.com/gorilla/mux"
    gocid "github.com/ipfs/go-cid"
    "openhashdb/core/cidutil"
    "openhashdb/core/hasher"
)

// parseHashOrCID resolves a hash from either a `hash` or `cid` variable/query.
func parseHashOrCID(r *http.Request) (hasher.Hash, string, error) {
    vars := mux.Vars(r)
    if cidStr, ok := vars["cid"]; ok && cidStr != "" {
        c, err := gocid.Parse(cidStr)
        if err != nil { return hasher.Hash{}, "", fmt.Errorf("invalid cid: %w", err) }
        h, err := cidutil.ToHash(c)
        if err != nil { return hasher.Hash{}, "", fmt.Errorf("unsupported cid: %w", err) }
        return h, cidStr, nil
    }
    // query param support
    if cidStr := r.URL.Query().Get("cid"); cidStr != "" {
        c, err := gocid.Parse(cidStr)
        if err != nil { return hasher.Hash{}, "", fmt.Errorf("invalid cid: %w", err) }
        h, err := cidutil.ToHash(c)
        if err != nil { return hasher.Hash{}, "", fmt.Errorf("unsupported cid: %w", err) }
        return h, cidStr, nil
    }
    if hashStr, ok := vars["hash"]; ok && hashStr != "" {
        h, err := hasher.HashFromString(hashStr)
        if err != nil { return hasher.Hash{}, "", err }
        // compute a CID for response convenience (raw codec)
        c, _ := cidutil.FromHash(h, cidutil.Raw)
        return h, c.String(), nil
    }
    return hasher.Hash{}, "", fmt.Errorf("missing identifier")
}

