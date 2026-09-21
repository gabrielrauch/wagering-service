package sqs

import (
	"maps"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestAttributesRoundTrip(t *testing.T) {
	// The names a trace context actually travels under, because those are the
	// ones that have to survive: a package that carried "a" and "b" would not
	// have shown that a period or a hyphen in a name is allowed.
	original := map[string]string{
		"traceparent":  "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":   "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7",
		"baggage":      "correlationId=8f14e45f,tenant=provider-a",
		"x-request.id": "8f14e45f-ea6f-4f7b-9f3a-0d2bd9e73c11",
	}
	if err := checkAttributes(original); err != nil {
		t.Fatalf("check the attributes: %v", err)
	}
	encoded := encodeAttributes(original)
	for name, value := range encoded {
		if aws.ToString(value.DataType) != stringType {
			t.Errorf("%s travelled as %q, want %q", name, aws.ToString(value.DataType),
				stringType)
		}
	}
	decoded := decodeAttributes(encoded)
	if !maps.Equal(original, decoded) {
		t.Errorf("round trip = %v, want %v", decoded, original)
	}
}

func TestEncodeAttributesOfNothingIsAbsent(t *testing.T) {
	if encoded := encodeAttributes(nil); encoded != nil {
		t.Errorf("encode(nil) = %v, want nothing on the wire", encoded)
	}
	if encoded := encodeAttributes(map[string]string{}); encoded != nil {
		t.Errorf("encode(empty) = %v, want nothing on the wire", encoded)
	}
	if decoded := decodeAttributes(nil); decoded != nil {
		t.Errorf("decode(nil) = %v, want nil", decoded)
	}
}

// TestDecodeDropsWhatItCannotRepresent holds the reticence: a message somebody
// else put on the queue carrying a binary attribute is still a message this
// service can act on, because the body is what it acts on.
func TestDecodeDropsWhatItCannotRepresent(t *testing.T) {
	decoded := decodeAttributes(map[string]types.MessageAttributeValue{
		"traceparent": {DataType: aws.String(stringType), StringValue: aws.String("00-abc-def-01")},
		"blob":        {DataType: aws.String("Binary"), BinaryValue: []byte{0x00, 0x01}},
	})
	want := map[string]string{"traceparent": "00-abc-def-01"}
	if !maps.Equal(decoded, want) {
		t.Errorf("decoded = %v, want %v", decoded, want)
	}

	onlyBinary := decodeAttributes(map[string]types.MessageAttributeValue{
		"blob": {DataType: aws.String("Binary"), BinaryValue: []byte{0x00}},
	})
	if onlyBinary != nil {
		t.Errorf("decoded = %v, want nil when nothing was readable", onlyBinary)
	}
}

func TestAttributeRefusals(t *testing.T) {
	cases := []struct {
		name       string
		attributes map[string]string
		because    string
	}{
		{
			name:       "no name at all",
			attributes: map[string]string{"": "value"},
			because:    "needs a name",
		},
		{
			name:       "a name SQS reserves for itself",
			attributes: map[string]string{"AWS.TraceHeader": "value"},
			because:    "prefix SQS reserves",
		},
		{
			name:       "the other reserved prefix",
			attributes: map[string]string{"Amazon.Whatever": "value"},
			because:    "prefix SQS reserves",
		},
		{
			name:       "a leading period",
			attributes: map[string]string{".traceparent": "value"},
			because:    "misplaces a period",
		},
		{
			name:       "a trailing period",
			attributes: map[string]string{"traceparent.": "value"},
			because:    "misplaces a period",
		},
		{
			name:       "two periods together",
			attributes: map[string]string{"trace..parent": "value"},
			because:    "misplaces a period",
		},
		{
			name:       "a character SQS does not allow",
			attributes: map[string]string{"trace parent": "value"},
			because:    "does not allow",
		},
		{
			name:       "a name past the limit",
			attributes: map[string]string{strings.Repeat("a", 257): "value"},
			because:    "longer than 256 bytes",
		},
		{
			name:       "a value that is not there",
			attributes: map[string]string{"traceparent": ""},
			because:    "has no value",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkAttributes(c.attributes)
			if err == nil {
				t.Fatalf("checkAttributes(%v) = nil, want a refusal", c.attributes)
			}
			if !strings.Contains(err.Error(), c.because) {
				t.Errorf("error = %q, want it to say %q", err, c.because)
			}
		})
	}
}

// TestAttributesSize pins the calculation SQS charges by, because the batch
// chunking is only as correct as this is: four bytes of length for the name,
// four for the data type, one transport flag, four for the value, plus the
// bytes themselves.
func TestAttributesSize(t *testing.T) {
	if got := attributesSize(nil); got != 0 {
		t.Errorf("no attributes = %d bytes, want 0", got)
	}
	// "id" (2) + "String" (6) + "x" (1) = 9 bytes of content, plus 3 length
	// prefixes of 4 and one transport flag = 13.
	if got, want := attributesSize(map[string]string{"id": "x"}), 9+13; got != want {
		t.Errorf("one attribute = %d bytes, want %d", got, want)
	}
	two := attributesSize(map[string]string{"id": "x", "ab": "y"})
	if want := 2 * (9 + 13); two != want {
		t.Errorf("two attributes = %d bytes, want %d", two, want)
	}
}
