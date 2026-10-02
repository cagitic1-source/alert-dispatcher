package domain

import "errors"

var (
	ErrAlertNotFound = errors.New("alert not found")
	ErrInvalidInput  = errors.New("invalid input")
)
