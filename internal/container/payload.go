package container

import "fmt"

func ReadPayloadAt(c Container, offset int64, size int64) ([]byte, error) {
	if size < 0 {
		return nil, fmt.Errorf("invalid payload size: %d", size)
	}
	if c == nil {
		return nil, fmt.Errorf("container is nil")
	}
	if offset < ContainerHdrLen {
		return nil, fmt.Errorf("invalid payload offset before container header: %d", offset)
	}
	if err := validateContainerRange("payload read", offset, size, c.Size()); err != nil {
		return nil, err
	}

	payload, err := c.ReadAt(offset, size)
	if err != nil {
		return nil, fmt.Errorf("read payload at offset %d size %d: %w", offset, size, err)
	}

	return payload, nil
}
