package internal

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	fieldHeaders = "headers"
	fieldBody    = "body"
)

var (
	errMissingHeaders = errors.New("redisstreams: stream entry is missing headers")
	errMissingBody    = errors.New("redisstreams: stream entry is missing body")
	errInvalidHeaders = errors.New("redisstreams: stream entry headers are invalid")
	errInvalidBody    = errors.New("redisstreams: stream entry body is invalid")
)

// streamValues stores a JSON headers object and the raw message body. Redis
// strings are binary-safe, so the body is not encoded again.
func streamValues(headers map[string]string, body []byte) (map[string]any, error) {
	if headers == nil {
		headers = map[string]string{}
	}
	rawHeaders, err := json.Marshal(headers)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		fieldHeaders: string(rawHeaders),
		fieldBody:    body,
	}, nil
}

func entryMessage(values map[string]any) (map[string]string, []byte, error) {
	rawHeaders, ok := values[fieldHeaders]
	if !ok {
		return nil, nil, errMissingHeaders
	}
	headerText, err := fieldString(rawHeaders)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", errInvalidHeaders, err)
	}
	var headers map[string]string
	if err = json.Unmarshal([]byte(headerText), &headers); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", errInvalidHeaders, err)
	}

	rawBody, ok := values[fieldBody]
	if !ok {
		return nil, nil, errMissingBody
	}
	body, err := fieldBytes(rawBody)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", errInvalidBody, err)
	}
	return headers, body, nil
}

func fieldString(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	default:
		return "", fmt.Errorf("unsupported field type %T", value)
	}
}

func fieldBytes(value any) ([]byte, error) {
	switch typed := value.(type) {
	case string:
		return []byte(typed), nil
	case []byte:
		return typed, nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported field type %T", value)
	}
}
