package sqs

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// stringType is the only message attribute data type this package writes.
//
// Trace context is text — a W3C traceparent, a tracestate, a baggage header —
// and writing it as Binary would make it unreadable in the console at the
// moment somebody is trying to follow a request through the queue by eye.
const stringType = "String"

// The rules SQS applies to a message attribute name, restated so that a name
// that would be refused is refused here, with the offending name in the
// sentence, rather than by a batch call that takes nine innocent messages down
// with it.
const (
	maxAttributeNameBytes = 256
	// reservedPrefixes are the namespaces SQS keeps for itself.
	awsPrefix    = "AWS."
	amazonPrefix = "Amazon."
)

// checkAttributes refuses a set of message attributes SQS would refuse.
//
// It is called before anything is sent rather than after something is refused,
// because SendMessageBatch refuses the whole request for a malformed attribute
// name — so one bad name in a batch of ten is nine messages that did not go out
// and a publisher with no way to tell which of them was the problem.
func checkAttributes(attributes map[string]string) error {
	for name, value := range attributes {
		if err := checkAttributeName(name); err != nil {
			return err
		}
		if value == "" {
			return fmt.Errorf("sqs: the message attribute %q has no value, which SQS refuses",
				name)
		}
	}
	return nil
}

// checkAttributeName holds a name to what SQS accepts.
func checkAttributeName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("sqs: a message attribute needs a name")
	case len(name) > maxAttributeNameBytes:
		return fmt.Errorf("sqs: the message attribute name %q is longer than %d bytes",
			name, maxAttributeNameBytes)
	case strings.HasPrefix(name, awsPrefix), strings.HasPrefix(name, amazonPrefix):
		return fmt.Errorf("sqs: the message attribute name %q uses a prefix SQS reserves", name)
	case strings.HasPrefix(name, "."), strings.HasSuffix(name, "."),
		strings.Contains(name, ".."):
		return fmt.Errorf("sqs: the message attribute name %q misplaces a period", name)
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '_', r == '-', r == '.':
		default:
			return fmt.Errorf("sqs: the message attribute name %q contains %q, which SQS does "+
				"not allow", name, r)
		}
	}
	return nil
}

// encodeAttributes turns a flat set of names and values into what SQS carries.
//
// Nothing here interprets a name. This package exists to carry trace context
// across the queue, not to understand it, and a tracing integration that knows
// what a traceparent means is a later task's — what has to exist now is the
// carriage, so that adding the meaning later is not also a change to the wire.
func encodeAttributes(attributes map[string]string) map[string]types.MessageAttributeValue {
	if len(attributes) == 0 {
		// nil rather than an empty map: SQS treats an empty attribute map as
		// absent anyway, and sending one would put an empty object in every
		// request for nothing.
		return nil
	}
	encoded := make(map[string]types.MessageAttributeValue, len(attributes))
	for name, value := range attributes {
		encoded[name] = types.MessageAttributeValue{
			DataType:    aws.String(stringType),
			StringValue: aws.String(value),
		}
	}
	return encoded
}

// decodeAttributes reads message attributes back into the form they were given
// in.
//
// An attribute with no string value is dropped rather than reported. That is
// the Binary case, which this package never writes and has no representation
// for, and a message somebody else put on the queue carrying one is not a
// reason to refuse the message — the body is what this service acts on.
func decodeAttributes(attributes map[string]types.MessageAttributeValue) map[string]string {
	if len(attributes) == 0 {
		return nil
	}
	decoded := make(map[string]string, len(attributes))
	for name, value := range attributes {
		if value.StringValue == nil {
			continue
		}
		decoded[name] = *value.StringValue
	}
	if len(decoded) == 0 {
		return nil
	}
	return decoded
}

// The overheads SQS adds when it charges a message attribute against the size
// limit, as its own documentation states the calculation.
const (
	// lengthPrefixBytes is charged three times per attribute — once each for
	// the name, the data type and the value.
	lengthPrefixBytes = 4
	// transportFlagBytes is the one byte that says whether the value travelled
	// as a string or as binary.
	transportFlagBytes = 1
)

// attributesSize is what a set of message attributes counts for against the
// 256 KiB a message and a batch are each allowed.
//
// Computing it here rather than sending and finding out is the point of the
// whole chunking exercise: SQS refuses an oversized batch as a unit, so a
// publisher that discovered the limit from the service would lose nine
// good messages to one that was too big.
func attributesSize(attributes map[string]string) int {
	total := 0
	for name, value := range attributes {
		total += lengthPrefixBytes + len(name)
		total += lengthPrefixBytes + len(stringType)
		total += transportFlagBytes
		total += lengthPrefixBytes + len(value)
	}
	return total
}
