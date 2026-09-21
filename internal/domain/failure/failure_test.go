package failure

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// declared lists every code the package exports. It is written out by hand so
// that adding a constant without classifying it fails a test rather than
// silently defaulting to "settled".
var declared = []Code{
	UninitializedValue,
	InvalidAmountFormat,
	InvalidAmountScale,
	AmountOutOfRange,
	InvalidAmountForKind,
	UnsupportedCurrency,
	MissingRequiredField,
	InvalidFieldFormat,
	UnsupportedTransactionKind,
	ReferenceRequired,
	ReferenceNotApplicable,
	InvalidStateTransition,
	InsufficientFunds,
	ReversalInsufficientFunds,
	BalanceOutOfRange,
	CurrencyMismatch,
	ReferenceNotFound,
	ReferenceNotProcessed,
	ReferenceNotReversible,
	ReferenceAlreadyReversed,
	ReferenceMismatch,
	ReversalAmountMismatch,
	IdempotencyPayloadConflict,
	WalletAlreadyExists,
	LedgerBalanceMismatch,
}

func TestCatalogueIsComplete(t *testing.T) {
	t.Parallel()

	if len(All()) != len(declared) {
		t.Fatalf("All() has %d codes but %d are declared; a code was added without being classified",
			len(All()), len(declared))
	}
	for _, code := range declared {
		if !code.Known() {
			t.Errorf("%s is declared but not classified", code)
		}
	}
}

// TestClassificationIsTotalAndExclusive pins the property the whole error model
// rests on: every code is correctable or definitive, and never both.
func TestClassificationIsTotalAndExclusive(t *testing.T) {
	t.Parallel()

	for _, code := range declared {
		if code.Correctable() == code.Definitive() {
			t.Errorf("%s is correctable=%v and definitive=%v; it must be exactly one",
				code, code.Correctable(), code.Definitive())
		}
	}
}

// TestAuditCodesAreDefinitiveWithoutSettlingAnything pins the second axis. An
// audit code reports corruption found in stored state, so it is never
// retryable, but nothing was in flight for it to settle either — and the two
// properties are separate, because the first is about the provider and the
// second is about whether a wager transaction may record it.
func TestAuditCodesAreDefinitiveWithoutSettlingAnything(t *testing.T) {
	t.Parallel()

	var audited int
	for _, code := range declared {
		if !code.Audit() {
			continue
		}
		audited++
		// An audit finding is not an invitation to retry.
		if code.Correctable() {
			t.Errorf("%s is an audit code but reports as correctable", code)
		}
		if !code.Definitive() {
			t.Errorf("%s is an audit code but does not report as definitive", code)
		}
	}
	if audited == 0 {
		t.Fatal("the catalogue declares no audit codes, so this rule cannot be tested")
	}

	// The axis has to separate something, or it is decoration.
	if !LedgerBalanceMismatch.Audit() {
		t.Error("LedgerBalanceMismatch is not marked as an audit code")
	}
	if InsufficientFunds.Audit() {
		t.Error("a settled business outcome reports as an audit code")
	}
	if MissingRequiredField.Audit() {
		t.Error("a malformed submission reports as an audit code")
	}
}

func TestUnknownCodeIsNeitherKnownNorCorrectable(t *testing.T) {
	t.Parallel()

	const unknown Code = "SOMETHING_ELSE"
	if unknown.Known() {
		t.Error("an undeclared code reports as known")
	}
	// An unrecognised failure must never be read as an invitation to retry.
	if unknown.Correctable() {
		t.Error("an undeclared code reports as correctable")
	}
	if unknown.Definitive() {
		t.Error("an undeclared code reports as definitive")
	}
	if unknown.Audit() {
		t.Error("an undeclared code reports as an audit code")
	}
}

// TestCodesAreDistinct guards against two constants sharing a wire value, which
// would make a documented reason ambiguous to a provider.
func TestCodesAreDistinct(t *testing.T) {
	t.Parallel()

	seen := make(map[Code]bool, len(declared))
	for _, code := range declared {
		if seen[code] {
			t.Errorf("%s is declared twice", code)
		}
		seen[code] = true
	}
}

// TestCodesAreScreamingSnakeCase keeps the wire contract predictable.
func TestCodesAreScreamingSnakeCase(t *testing.T) {
	t.Parallel()

	for _, code := range declared {
		s := string(code)
		if s == "" || s != strings.ToUpper(s) {
			t.Errorf("%q is not upper case", s)
		}
		for _, r := range s {
			if (r < 'A' || r > 'Z') && r != '_' {
				t.Errorf("%q contains %q, want only A-Z and underscore", s, r)
			}
		}
	}
}

func TestErrorFormatting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  *Error
		want string
	}{
		{"code only", New(InsufficientFunds, ""), "INSUFFICIENT_FUNDS"},
		{"code and message", New(InsufficientFunds, "balance is %s", "10.00"), "INSUFFICIENT_FUNDS: balance is 10.00"},
		{"code and field", New(MissingRequiredField, "").WithField("roundId"), "MISSING_REQUIRED_FIELD: roundId"},
		{
			"code, field and message",
			New(MissingRequiredField, "must be present").WithField("roundId"),
			"MISSING_REQUIRED_FIELD: roundId: must be present",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.err.Error(); got != tc.want {
				t.Errorf("Error() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestErrorsIsMatchesOnCode(t *testing.T) {
	t.Parallel()

	err := New(InsufficientFunds, "balance is too low")

	if !errors.Is(err, New(InsufficientFunds, "an entirely different message")) {
		t.Error("errors.Is did not match two errors carrying the same code")
	}
	if errors.Is(err, New(CurrencyMismatch, "balance is too low")) {
		t.Error("errors.Is matched two errors carrying different codes")
	}
}

func TestErrorsAsTypeExposesTheCode(t *testing.T) {
	t.Parallel()

	err := error(New(ReferenceMismatch, "round disagrees").WithField("roundId"))

	got, ok := errors.AsType[*Error](err)
	if !ok {
		t.Fatal("errors.AsType did not find the failure error")
	}
	if got.Code != ReferenceMismatch {
		t.Errorf("Code = %s, want %s", got.Code, ReferenceMismatch)
	}
	if got.Field != "roundId" {
		t.Errorf("Field = %q, want %q", got.Field, "roundId")
	}
}

func TestWrappingKeepsTheCauseReachable(t *testing.T) {
	t.Parallel()

	cause := errors.New("the underlying problem")
	err := Wrap(cause, AmountOutOfRange, "amount is too large")

	if !errors.Is(err, cause) {
		t.Error("the wrapped cause is not reachable through errors.Is")
	}
	if code, ok := CodeOf(err); !ok || code != AmountOutOfRange {
		t.Errorf("CodeOf = %s, %v, want %s, true", code, ok, AmountOutOfRange)
	}
}

// TestCodeSurvivesFurtherWrapping covers the realistic case of a domain error
// travelling up through a layer that adds its own context.
func TestCodeSurvivesFurtherWrapping(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("applying the operation: %w", New(InsufficientFunds, "balance is too low"))

	if !Is(err, InsufficientFunds) {
		t.Error("the code was lost when the error was wrapped again")
	}
	if !Correctable(err) == InsufficientFunds.Correctable() {
		t.Error("Correctable disagreed with the code it reports")
	}
}

func TestWithFieldDoesNotMutateTheReceiver(t *testing.T) {
	t.Parallel()

	original := New(MissingRequiredField, "must be present")
	annotated := original.WithField("gameId")

	if original.Field != "" {
		t.Errorf("WithField mutated the receiver, whose field is now %q", original.Field)
	}
	if annotated.Field != "gameId" {
		t.Errorf("Field = %q, want %q", annotated.Field, "gameId")
	}
	if annotated.Code != original.Code {
		t.Error("WithField changed the code")
	}
}

func TestCodeOfAndCorrectableRejectForeignErrors(t *testing.T) {
	t.Parallel()

	foreign := errors.New("something from another package")

	if _, ok := CodeOf(foreign); ok {
		t.Error("CodeOf found a code in an unrelated error")
	}
	// An unrecognised error is never an invitation to retry.
	if Correctable(foreign) {
		t.Error("an unrelated error reports as correctable")
	}
	if Correctable(nil) {
		t.Error("a nil error reports as correctable")
	}
}

// Message is the refusal's own words, without the code and the field that Error
// renders in front of them. It exists so a caller that states those itself does
// not print them twice.
func TestMessageIsTheRefusalWithoutItsRenderedHead(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		err         *Error
		wantMessage string
		wantError   string
	}{
		{
			name:        "code, field and message",
			err:         New(InsufficientFunds, "balance 10.00 is below 25.00").WithField("amount"),
			wantMessage: "balance 10.00 is below 25.00",
			wantError:   "INSUFFICIENT_FUNDS: amount: balance 10.00 is below 25.00",
		},
		{
			name:        "code and message",
			err:         New(InsufficientFunds, "balance 10.00 is below 25.00"),
			wantMessage: "balance 10.00 is below 25.00",
			wantError:   "INSUFFICIENT_FUNDS: balance 10.00 is below 25.00",
		},
		{
			name:        "code alone",
			err:         New(InsufficientFunds, ""),
			wantMessage: "",
			wantError:   "INSUFFICIENT_FUNDS",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.err.Message(); got != tc.wantMessage {
				t.Errorf("Message() = %q, want %q", got, tc.wantMessage)
			}
			if got := tc.err.Error(); got != tc.wantError {
				t.Errorf("Error() = %q, want %q", got, tc.wantError)
			}
		})
	}
}

// A nil *Error renders nothing rather than panicking, which is the same
// courtesy WithField already extends.
func TestMessageOnANilErrorIsEmpty(t *testing.T) {
	t.Parallel()
	var e *Error
	if got := e.Message(); got != "" {
		t.Errorf("Message() = %q, want %q", got, "")
	}
}
