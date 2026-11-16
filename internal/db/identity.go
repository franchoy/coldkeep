package db

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"regexp"
)

// Identity names are typed, opaque 128-bit CSPRNG values. They convey no
// permission, physical binding or historical device possession.
type RepositoryID string
type RootID string
type DeviceID string
type SourceID string
type AdoptionID string
type GenerationID string
type EntryID string
type PolicyID string
type UpgradeID string

var ErrIdentityKind = errors.New("unsupported identity kind")

var identityPattern = regexp.MustCompile(`^(repo|root|device|source|adopt|generation|entry|policy|upgrade)_[0-9a-f]{32}$`)

func ValidIdentity(kind, value string) bool {
	return len(value) == len(kind)+33 && identityPattern.MatchString(value) && value[:len(kind)] == kind
}

func NewIdentity(kind string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	value := kind + "_" + hex.EncodeToString(raw[:])
	if !ValidIdentity(kind, value) {
		return "", ErrIdentityKind
	}
	return value, nil
}
