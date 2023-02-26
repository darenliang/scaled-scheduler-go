package interfaces

import (
	"go.uber.org/zap/zapcore"
)

// Serializable is an interface that allows a struct to be serialized into a byte array.
type Serializable interface {
	zapcore.ObjectMarshaler
	Serialize() [][]byte
}

// Stringer is an interface that allows enums to be casted into a string.
type Stringer interface {
	String() string
}
