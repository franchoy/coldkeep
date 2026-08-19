package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func (s *Service) GetRepositoryConfiguration(ctx context.Context, key string) (RepositoryConfigurationRef, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return RepositoryConfigurationRef{}, NewError(ErrorInvalidArgument, "get repository configuration", "configuration_key_nonblank", "configuration key is required", nil)
	}
	if err := ctx.Err(); err != nil {
		return RepositoryConfigurationRef{}, repositoryConfigurationError("get repository configuration", err)
	}
	ref := RepositoryConfigurationRef{Key: key}
	err := s.db.QueryRowContext(ctx, `SELECT value FROM repository_config WHERE key = $1`, key).Scan(&ref.Value)
	if errors.Is(err, sql.ErrNoRows) {
		return ref, nil
	}
	if err != nil {
		return RepositoryConfigurationRef{}, repositoryConfigurationError("get repository configuration", err)
	}
	ref.Exists = true
	return ref, nil
}

func (s *Service) SetRepositoryConfiguration(ctx context.Context, key, value string) (SetRepositoryConfigurationResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return SetRepositoryConfigurationResult{}, NewError(ErrorInvalidArgument, "set repository configuration", "configuration_key_nonblank", "configuration key is required", nil)
	}
	if err := ctx.Err(); err != nil {
		return SetRepositoryConfigurationResult{}, repositoryConfigurationError("set repository configuration", err)
	}
	previous, err := s.GetRepositoryConfiguration(ctx, key)
	if err != nil {
		return SetRepositoryConfigurationResult{}, err
	}
	result := SetRepositoryConfigurationResult{
		Key: key, Value: value, PreviousValue: previous.Value,
		PreviouslySet: previous.Exists, Changed: !previous.Exists || previous.Value != value,
	}
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO repository_config(key, value)
VALUES($1, $2)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value); err != nil {
		return SetRepositoryConfigurationResult{}, repositoryConfigurationError("set repository configuration", err)
	}
	return result, nil
}

func repositoryConfigurationError(operation string, err error) error {
	code := ErrorOperationFailed
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		code = ErrorCancelled
	}
	return NewError(code, operation, "", fmt.Sprintf("%s failed", operation), err)
}
