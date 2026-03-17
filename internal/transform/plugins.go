package transform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/google/cel-go/common/types/ref"
)

type dropIfStep struct {
	expr *compiledExpression
}

func buildDropIfStep(step StepConfig, compile CompileFunc) (Step, error) {
	if step.Expr == "" {
		return nil, errors.New("expr is required")
	}
	expr, err := compile(step.Expr)
	if err != nil {
		return nil, err
	}
	return &dropIfStep{expr: expr}, nil
}

func (s *dropIfStep) Apply(ctx context.Context, msg *Message) (Action, error) {
	raw, err := s.expr.Eval(ctx, msg)
	if err != nil {
		return ActionContinue, err
	}

	result, ok := raw.(bool)
	if !ok {
		return ActionContinue, fmt.Errorf("drop_if expects bool result, got %T", raw)
	}
	if result {
		return ActionDrop, nil
	}
	return ActionContinue, nil
}

type rewriteValueStep struct {
	expr *compiledExpression
}

func buildRewriteValueStep(step StepConfig, compile CompileFunc) (Step, error) {
	if step.Expr == "" {
		return nil, errors.New("expr is required")
	}
	expr, err := compile(step.Expr)
	if err != nil {
		return nil, err
	}
	return &rewriteValueStep{expr: expr}, nil
}

func (s *rewriteValueStep) Apply(ctx context.Context, msg *Message) (Action, error) {
	raw, err := s.expr.Eval(ctx, msg)
	if err != nil {
		return ActionContinue, err
	}

	payload, err := json.Marshal(normalizeValue(raw))
	if err != nil {
		return ActionContinue, fmt.Errorf("marshal rewrite_value result: %w", err)
	}
	msg.Value = payload
	return ActionContinue, nil
}

type setKeyStep struct {
	expr *compiledExpression
}

func buildSetKeyStep(step StepConfig, compile CompileFunc) (Step, error) {
	if step.Expr == "" {
		return nil, errors.New("expr is required")
	}
	expr, err := compile(step.Expr)
	if err != nil {
		return nil, err
	}
	return &setKeyStep{expr: expr}, nil
}

func (s *setKeyStep) Apply(ctx context.Context, msg *Message) (Action, error) {
	raw, err := s.expr.Eval(ctx, msg)
	if err != nil {
		return ActionContinue, err
	}
	if raw == nil {
		msg.Key = nil
		return ActionContinue, nil
	}

	text, err := valueToString(raw)
	if err != nil {
		return ActionContinue, err
	}
	msg.Key = []byte(text)
	return ActionContinue, nil
}

type headerExpr struct {
	key  string
	expr *compiledExpression
}

type setHeadersStep struct {
	headers []headerExpr
}

func buildSetHeadersStep(step StepConfig, compile CompileFunc) (Step, error) {
	if len(step.Headers) == 0 {
		return nil, errors.New("headers is required")
	}

	keys := make([]string, 0, len(step.Headers))
	for key := range step.Headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	expressions := make([]headerExpr, 0, len(keys))
	for _, key := range keys {
		headerKey := strings.TrimSpace(key)
		exprText := strings.TrimSpace(step.Headers[key])
		if headerKey == "" {
			return nil, errors.New("header key is empty")
		}
		if exprText == "" {
			return nil, fmt.Errorf("header %q expression is empty", headerKey)
		}

		expr, err := compile(exprText)
		if err != nil {
			return nil, fmt.Errorf("compile header %q expression: %w", headerKey, err)
		}
		expressions = append(expressions, headerExpr{
			key:  headerKey,
			expr: expr,
		})
	}

	return &setHeadersStep{headers: expressions}, nil
}

func (s *setHeadersStep) Apply(ctx context.Context, msg *Message) (Action, error) {
	for _, header := range s.headers {
		raw, err := header.expr.Eval(ctx, msg)
		if err != nil {
			return ActionContinue, err
		}

		text, err := valueToString(raw)
		if err != nil {
			return ActionContinue, fmt.Errorf("header %q value: %w", header.key, err)
		}
		msg.SetHeader(header.key, []byte(text))
	}

	return ActionContinue, nil
}

func valueToString(value any) (string, error) {
	value = normalizeValue(value)

	switch typed := value.(type) {
	case nil:
		return "", nil
	case string:
		return typed, nil
	case json.Number:
		return typed.String(), nil
	case bool:
		if typed {
			return "true", nil
		}
		return "false", nil
	case float64, float32, int, int64, int32, int16, int8, uint, uint64, uint32, uint16, uint8:
		return fmt.Sprint(typed), nil
	case []byte:
		return string(typed), nil
	}

	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("convert result type %T to string: %w", value, err)
	}
	return string(payload), nil
}

func normalizeValue(value any) any {
	if value == nil {
		return nil
	}

	if celValue, ok := value.(ref.Val); ok {
		return normalizeValue(celValue.Value())
	}

	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, val := range typed {
			result[key] = normalizeValue(val)
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for idx := range typed {
			result[idx] = normalizeValue(typed[idx])
		}
		return result
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Map:
		result := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			key := fmt.Sprint(normalizeValue(iter.Key().Interface()))
			result[key] = normalizeValue(iter.Value().Interface())
		}
		return result
	case reflect.Slice, reflect.Array:
		result := make([]any, rv.Len())
		for idx := 0; idx < rv.Len(); idx++ {
			result[idx] = normalizeValue(rv.Index(idx).Interface())
		}
		return result
	}

	return value
}
