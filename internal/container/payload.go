package container

import "fmt"

func ReadPayloadAt(c Container, offset int64, size int64) ([]byte, error) {
	if size < 0 {
		return nil, fmt.Errorf("invalid payload size: %d", size)
	}

	payload, err := c.ReadAt(offset, size)
	if err != nil {
		return nil, fmt.Errorf("read payload at offset %d size %d: %w", offset, size, err)
	}

	return payload, nil
}
