package sqs

import "github.com/nferreira/adapter/pkg/adapter"

type ErrorMapping map[error]int

type BindingRule struct {
}

func NewBindingRule() adapter.BindingRule {
	return &BindingRule{
	}
}
