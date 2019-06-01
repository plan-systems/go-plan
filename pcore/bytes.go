package pcore





// Marshaller used to generalize serialization
type Marshaller interface {
	Marshal() ([]byte, error)
    MarshalTo([]byte) (int, error)
    Size() int

	Unmarshal([]byte) error
}
