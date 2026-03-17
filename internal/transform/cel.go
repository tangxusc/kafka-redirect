package transform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
)

type celCompiler struct {
	env *cel.Env
}

type compiledExpression struct {
	source  string
	program cel.Program
}

func newCELCompiler() (*celCompiler, error) {
	env, err := cel.NewEnv(
		cel.Variable("key", cel.StringType),
		cel.Variable("value", cel.DynType),
		cel.Variable("headers", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("topic", cel.StringType),
		cel.Variable("partition", cel.IntType),
		cel.Variable("offset", cel.IntType),
		cel.Variable("timestamp", cel.StringType),
	)
	if err != nil {
		return nil, fmt.Errorf("create cel env: %w", err)
	}
	return &celCompiler{env: env}, nil
}

func (c *celCompiler) Compile(expr string) (*compiledExpression, error) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return nil, errors.New("expression is empty")
	}

	ast, issues := c.env.Compile(trimmed)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("compile expression %q: %w", trimmed, issues.Err())
	}

	program, err := c.env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("build expression program %q: %w", trimmed, err)
	}

	return &compiledExpression{
		source:  trimmed,
		program: program,
	}, nil
}

func (expr *compiledExpression) Eval(ctx context.Context, msg *Message) (any, error) {
	value, err := decodeJSONValue(msg.Value)
	if err != nil {
		return nil, fmt.Errorf("decode message value json: %w", err)
	}

	timestamp := ""
	if !msg.Timestamp.IsZero() {
		timestamp = msg.Timestamp.UTC().Format(time.RFC3339Nano)
	}

	out, _, err := expr.program.ContextEval(ctx, map[string]any{
		"key":       string(msg.Key),
		"value":     value,
		"headers":   msg.HeaderStringMap(),
		"topic":     msg.Topic,
		"partition": int64(msg.Partition),
		"offset":    msg.Offset,
		"timestamp": timestamp,
	})
	if err != nil {
		return nil, fmt.Errorf("evaluate expression %q: %w", expr.source, err)
	}

	return out.Value(), nil
}

func decodeJSONValue(raw []byte) (any, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, errors.New("value is empty")
	}

	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}

	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return nil, errors.New("value contains trailing content")
	} else if !errors.Is(err, io.EOF) {
		return nil, err
	}

	return value, nil
}
