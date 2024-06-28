package loader

import "fmt"

type Status struct {
	FromBundleId int64
	ToBundleId   int64
	FromKey      string
	ToKey        string
	DataSize     int64
}

func (s Status) String() string {
	return fmt.Sprintf(
		"Ids: (%d - %d), Keys: (%s - %s)",
		s.FromBundleId,
		s.ToBundleId,
		s.FromKey,
		s.ToKey,
	)
}
