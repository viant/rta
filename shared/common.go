package shared

const (
	// Active status means that the record in a journal table was not processed (data was not merged)
	Active = 1
	// InActive status means that the record in a journal table was processed (data was merged)
	InActive = 0
	Deleted  = -1
)
