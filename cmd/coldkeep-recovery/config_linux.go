package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"unicode/utf8"

	"github.com/franchoy/coldkeep/v2/internal/recoveryexport"
)

type config struct {
	PayloadRootBase64 string `json:"payload_root_base64"`
	Host              string `json:"host"`
	Port              uint16 `json:"port"`
	Database          string `json:"database"`
	User              string `json:"user"`
	PayloadRoot       string `json:"payload_root"`
	PasswordFile      string `json:"password_file"`
	KeyFile           string `json:"key_file"`
	KeyReference      string `json:"key_reference"`
	PGDump            string `json:"pg_dump"`
	PGRestore         string `json:"pg_restore"`
}

func privateCredentialInfo(info os.FileInfo, max int64) bool {
	return info.Mode().IsRegular() && info.Mode().Perm()&0077 == 0 && info.Size() <= max
}

func readRecoveryConfig(path string) (config, error) {
	var c config
	if e := readJSON(path, &c); e != nil {
		return c, e
	}
	if c.PayloadRootBase64 != "" {
		if c.PayloadRoot != "" {
			return c, recoveryexport.ErrConfiguration
		}
		path, e := recoveryexport.DecodeBindingPath(c.PayloadRootBase64)
		if e != nil {
			return c, e
		}
		c.PayloadRoot = path
	}
	return c, nil
}

func privateBytes(path string, max int64) ([]byte, error) {
	info, e := os.Lstat(path)
	if e != nil || !privateCredentialInfo(info, max) {
		return nil, errors.New("private regular credential file required")
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	after, e := f.Stat()
	if e != nil || !os.SameFile(info, after) {
		return nil, errors.Join(errors.New("credential identity changed"), f.Close())
	}
	b, e := io.ReadAll(io.LimitReader(f, max+1))
	e = errors.Join(e, f.Close())
	if int64(len(b)) > max {
		return nil, errors.New("credential limit")
	}
	return b, e
}
func readJSON(path string, v any) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	// Read a sentinel byte as well: a decoder behind a reader limited to the
	// ceiling cannot distinguish actual EOF from excess trailing whitespace.
	b, e := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if e = errors.Join(e, f.Close()); e != nil {
		return e
	}
	if len(b) > 1<<20 {
		return errors.New("operator configuration limit")
	}
	if !losslessConfigUnicode(b) {
		return errors.New("invalid configuration Unicode")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	e = d.Decode(v)
	if e == nil {
		var extra any
		if d.Decode(&extra) != io.EOF {
			e = errors.New("trailing JSON")
		}
	}
	return e
}

// encoding/json replaces invalid UTF-8 and unpaired UTF-16 escapes. Check
// those representations before decoding any field that can select authority.
// This bounded linear scan leaves ordinary JSON syntax to the decoder.
func losslessConfigUnicode(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	inString := false
	for i := 0; i < len(b); i++ {
		switch b[i] {
		case '"':
			inString = !inString
		case '\\':
			if !inString {
				continue
			}
			width, ok := configEscapeWidth(b[i:])
			if !ok {
				return false
			}
			i += width - 1
		}
	}
	return true
}

func configUnicodeEscape(b []byte) (uint64, bool) {
	if len(b) < 6 || b[0] != '\\' || b[1] != 'u' {
		return 0, false
	}
	unit, e := strconv.ParseUint(string(b[2:6]), 16, 16)
	return unit, e == nil
}

// Width includes the backslash. Ordinary escapes, including a trailing
// backslash, remain the JSON decoder's responsibility.
func configEscapeWidth(b []byte) (int, bool) {
	if len(b) < 2 || b[1] != 'u' {
		return 2, true
	}
	unit, ok := configUnicodeEscape(b)
	if !ok || configLowSurrogate(unit) {
		return 0, false
	}
	if unit < 0xd800 || unit > 0xdbff {
		return 6, true
	}
	unit, ok = configUnicodeEscape(b[6:])
	return 12, ok && configLowSurrogate(unit)
}

func configLowSurrogate(unit uint64) bool {
	return unit >= 0xdc00 && unit <= 0xdfff
}
