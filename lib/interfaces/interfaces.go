package interfaces

type Serializable interface {
	Serialize() [][]byte
}

type Stringer interface {
	String() string
}
