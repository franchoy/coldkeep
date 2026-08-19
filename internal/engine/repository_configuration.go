package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/storage"
)

const (
	catalogDefaultChunkerKey   = "default_chunker"
	catalogCompressionKey      = "compression"
	catalogCompressionLevelKey = "compression_level"
	defaultConfigurationCodec  = "none"
	defaultConfigurationLevel  = int64(3)
	minimumCompressionLevel    = int64(1)
	maximumCompressionLevel    = int64(9)
)

func (e *DefaultEngine) GetConfiguration(ctx context.Context, req GetConfigurationRequest) (_ GetConfigurationResult, outErr error) {
	defer func() { outErr = TranslateError("get configuration", outErr) }()
	catalogKey, err := configurationCatalogKey(req.Key)
	if err != nil {
		return GetConfigurationResult{}, TranslateErrorAs("get configuration", ErrorInvalidArgument, err)
	}
	ref, err := catalog.NewServiceFromSQL(e.config.DB).GetRepositoryConfiguration(ctx, catalogKey)
	if err != nil {
		return GetConfigurationResult{}, TranslateError("get configuration", err)
	}
	value := ref.Value
	if !ref.Exists {
		value = configurationDefault(req.Key)
	}
	result, err := validatedConfigurationResult(req.Key, value)
	if err != nil {
		return GetConfigurationResult{}, TranslateError("get configuration", err)
	}
	return result, nil
}

func (e *DefaultEngine) SetConfiguration(ctx context.Context, req SetConfigurationRequest) (_ SetConfigurationResult, outErr error) {
	defer func() { outErr = TranslateError("set configuration", outErr) }()
	catalogKey, err := configurationCatalogKey(req.Key)
	if err != nil {
		return SetConfigurationResult{}, TranslateErrorAs("set configuration", ErrorInvalidArgument, err)
	}
	normalized, integerValue, err := e.normalizeConfigurationValue(req.Key, req.Value)
	if err != nil {
		return SetConfigurationResult{}, TranslateErrorAs("set configuration", ErrorInvalidArgument, err)
	}
	previous, err := e.GetConfiguration(ctx, GetConfigurationRequest{Key: req.Key})
	if err != nil {
		return SetConfigurationResult{}, err
	}
	if _, err := catalog.NewServiceFromSQL(e.config.DB).SetRepositoryConfiguration(ctx, catalogKey, normalized); err != nil {
		return SetConfigurationResult{}, TranslateError("set configuration", err)
	}
	return SetConfigurationResult{
		Key: req.Key, Value: normalized, IntegerValue: integerValue,
		Changed: previous.Value != normalized,
	}, nil
}

func configurationCatalogKey(key ConfigurationKey) (string, error) {
	switch key {
	case ConfigurationDefaultChunker:
		return catalogDefaultChunkerKey, nil
	case ConfigurationCompression:
		return catalogCompressionKey, nil
	case ConfigurationCompressionLevel:
		return catalogCompressionLevelKey, nil
	default:
		return "", fmt.Errorf("unknown config key: %s", key)
	}
}

func configurationDefault(key ConfigurationKey) string {
	switch key {
	case ConfigurationDefaultChunker:
		return string(chunk.DefaultChunkerVersion)
	case ConfigurationCompression:
		return defaultConfigurationCodec
	case ConfigurationCompressionLevel:
		return strconv.FormatInt(defaultConfigurationLevel, 10)
	default:
		return ""
	}
}

func validatedConfigurationResult(key ConfigurationKey, raw string) (GetConfigurationResult, error) {
	value := strings.TrimSpace(raw)
	result := GetConfigurationResult{Key: key, Value: value}
	switch key {
	case ConfigurationDefaultChunker:
		version := chunk.Version(value)
		if !chunk.IsWellFormedVersion(version) {
			return GetConfigurationResult{}, fmt.Errorf("repository default chunker version %q is malformed", version)
		}
		if _, ok := chunk.DefaultRegistry().Get(version); !ok {
			return GetConfigurationResult{}, fmt.Errorf("repository default chunker version validation failed: chunker version %q is not registered in this binary", version)
		}
	case ConfigurationCompression:
		if !storage.IsRegisteredCompressionCodec(value) {
			return GetConfigurationResult{}, fmt.Errorf("repository default compression codec %q is not registered", value)
		}
	case ConfigurationCompressionLevel:
		var level int64
		if _, err := fmt.Sscanf(value, "%d", &level); err != nil {
			return GetConfigurationResult{}, fmt.Errorf("repository default compression level %q is not a valid integer: %w", raw, err)
		}
		if level < minimumCompressionLevel || level > maximumCompressionLevel {
			return GetConfigurationResult{}, fmt.Errorf("repository default compression level %d is out of range [%d, %d]", level, minimumCompressionLevel, maximumCompressionLevel)
		}
		result.Value = strconv.FormatInt(level, 10)
		result.IntegerValue = int64Pointer(level)
	}
	return result, nil
}

func (e *DefaultEngine) normalizeConfigurationValue(key ConfigurationKey, raw string) (string, *int64, error) {
	value := strings.TrimSpace(raw)
	switch key {
	case ConfigurationDefaultChunker:
		version := chunk.Version(value)
		if !chunk.IsWellFormedVersion(version) {
			return "", nil, fmt.Errorf("invalid default-chunker value %q: malformed version", raw)
		}
		if _, ok := chunk.DefaultRegistry().Get(version); !ok {
			return "", nil, fmt.Errorf("invalid default-chunker value %q: unknown chunker version", raw)
		}
		if e.config.ChunkerDeprecationPolicy != nil {
			if deprecated, reason := e.config.ChunkerDeprecationPolicy(version); deprecated {
				reason = strings.TrimSpace(reason)
				if reason == "" {
					return "", nil, fmt.Errorf("invalid default-chunker value %q: deprecated chunker version", raw)
				}
				return "", nil, fmt.Errorf("invalid default-chunker value %q: deprecated chunker version (%s)", raw, reason)
			}
		}
		return string(version), nil, nil
	case ConfigurationCompression:
		if !storage.IsRegisteredCompressionCodec(value) {
			return "", nil, fmt.Errorf("invalid compression codec %q, must be 'none' or 'zstd'", value)
		}
		return value, nil, nil
	case ConfigurationCompressionLevel:
		level, err := strconv.Atoi(value)
		if err != nil {
			return "", nil, fmt.Errorf("invalid compression-level %q, must be an integer 1-9", value)
		}
		if level < int(minimumCompressionLevel) || level > int(maximumCompressionLevel) {
			return "", nil, fmt.Errorf("compression-level %d out of range, must be 1-9", level)
		}
		numeric := int64(level)
		return strconv.Itoa(level), &numeric, nil
	default:
		return "", nil, fmt.Errorf("unknown config key: %s", key)
	}
}

func int64Pointer(value int64) *int64 { return &value }
